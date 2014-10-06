package main

import akka.actor.ActorSystem
import akka.agent.Agent
import akka.stream.{MaterializerSettings, FlowMaterializer}
import akka.stream.scaladsl.Flow
import org.json4s.JsonAST.{JValue, JString}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.immutable._
import Util._
import dispatch._

import scala.io.Source
import scala.util.{Failure, Success}

object LinkListing {
  def fromJson(subreddit: String)(json: JValue) = {
    val x = json.\("data").\("children").children.map(_.\("data").\("id")).collect{ case JString(s) => Link(s, subreddit) }
    LinkListing(x)
  }
}
case class LinkListing(links: Seq[Link])
case class Link(id: String, subreddit: String)

object CommentListing {
  def fromJson(json: JValue) = {
    val x = json.\("data")
      .filterField{case ("body", _) => true; case _ => false }
      .collect{ case ("body", JString(s)) => Comment(s)}
    CommentListing(x)
  }
}
case class CommentListing(comments: Seq[Comment])
case class Comment(body: String){
  val alpha = (('a' to 'z') ++ ('A' to 'Z')).toSet

  def normalize(s: Seq[String]): Seq[String] =
    s.map(_.filter(alpha.contains).map(_.toLower)).filterNot(_.isEmpty)

  def words: Map[String, Int] =
    normalize(body.split(" ").to[Seq])
      .groupBy(identity)
      .mapValues(_.length)
}

object RedditAPI {
  def topLinks(subreddit: String)(implicit ec: ExecutionContext): Future[LinkListing] = {
    val page = url(s"http://www.reddit.com/r/$subreddit/top.json") <<? Map("limit" -> "25", "t" -> "all")
    val f = Http(page OK dispatch.as.json4s.Json).map(LinkListing.fromJson(subreddit)(_))
    timedFuture(s"links: r/$subreddit/top")(f)
  }

  def comments(link: Link)(implicit ec: ExecutionContext): Future[CommentListing] = {
    val page = url(s"http://www.reddit.com/r/${link.subreddit}/comments/${link.id}.json") <<? Map("depth" -> "25", "limit" -> "2000")
    val f = Http(page OK dispatch.as.json4s.Json).map(CommentListing.fromJson)
    timedFuture(s"comments: r/${link.subreddit}/${link.id}/comments")(f)
  }
}


object Util {
  def timedFuture[T](name: String)(f: Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = System.currentTimeMillis()
    println(s"--> send off $name")
    f.andThen{
      case Success(_) =>
        val end = System.currentTimeMillis()
        println(s"\t<-- completed $name, total time elapsed: ${end - start}")
      case Failure(ex) =>
        val end = System.currentTimeMillis()
        println(s"\t<X> failed $name, total time elapsed: ${end - start}\n$ex")
    }
  }
}

// jury-rigged value store starting with `zero` and merging in new elements as provided
case class Store[T](zero: T)(f: (T, T) => T )(implicit val ec: ExecutionContext) {
  private val store: Agent[T] = Agent(zero)

  // add the wordcount to the current one
  def update(in: T): Future[Unit] =
    store.alter(f(_, in)).map( _ => () )


  // get the value of `store` after all queued updates have been completed
  def read: Future[T] =
    store.future()
}



object WordCount {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  val settings = MaterializerSettings().withFanOut(16, 32).withBuffer(16, 32)
  implicit val mat = FlowMaterializer(settings)
  println(s"settings: $settings")

  def merge(a: Map[String, Int], b: Map[String, Int]): Map[String, Int] =
    b.foldLeft(a) { case (wc, (s, c)) => wc.updated(s, c + wc.getOrElse(s, 0))}

  val store = new Store(Map.empty[String, Int])(merge)

  val commentcountStore = new Store(0L)(_ + _)

  val subreddits = Vector("LifeProTips","explainlikeimfive","Jokes","askreddit", "funny", "news", "tifu")

  def main(args: Array[String]): Unit = {

    val f: Future[Unit] =
      Flow(subreddits) // start with a flow of subreddit names
      .mapFuture(RedditAPI.topLinks) // for each subreddit name, get a Future[LinkListing] and fold the result into the stream
      .mapConcat(_.links) // concat a Flow of listings of links into a Flow of links
      .mapFuture(RedditAPI.comments) // for each link, get a Future[CommentListing] and fold the result into the stream
      .mapConcat(_.comments) // concat a Flow of comment listings into a flow of comments
      .map(_.words) // get a wordcount for each
      .groupedWithin(200, 1 second) // group wordcounts to avoid excessive DB IO
      .mapFuture( xs => commentcountStore.update(xs.length).map(_ => xs ))
      .map(_.reduce(merge)) // merge wordcount lists
      .mapFuture(store.update) // send
      .fold(())( (_,_) => () ).toFuture // fold into nothing and grab the future

    timedFuture("main stream")(f).
      flatMap( _ => store.read.zip(commentcountStore.read) ).onComplete{
      case Success((finalWordCount, cc)) =>

        writeTsv(finalWordCount)

        println(s"$cc comments")

        println(s"${finalWordCount.size} distinct words, ${finalWordCount.values.sum} words total")
        as.shutdown()
      case _ =>

        as.shutdown()
    }
  }

  def writeTsv(wordcount: Map[String, Int]) = {

    val tsv = wordcount.toList.sortBy{ case (_, n) => n }.reverse.map{ case (s, n) => s"$s\t$n"}.mkString("\n")

    import java.nio.file.{Paths, Files}
    import java.nio.charset.StandardCharsets

    Files.write(Paths.get("words.tsv"), tsv.getBytes(StandardCharsets.UTF_8))
  }

}