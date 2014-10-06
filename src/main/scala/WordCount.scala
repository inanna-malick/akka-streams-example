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

  def toWordCount: Map[String, Int] =
    normalize(body.split(" ").to[Seq])
      .groupBy(identity)
      .mapValues(_.length)
}

object RedditAPI {
  def topLinks(subreddit: String)(implicit ec: ExecutionContext): Future[LinkListing] = {
    val page = url(s"http://www.reddit.com/r/$subreddit/top.json") <<? Map("limit" -> "100", "t" -> "all")
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
    println(s"--> started $name")
    f.andThen{
      case Success(_) =>
        val end = System.currentTimeMillis()
        println(s"\t<-- finished $name, total time elapsed: ${end - start}")
      case Failure(ex) =>
        val end = System.currentTimeMillis()
        println(s"\t<X> failed $name, total time elapsed: ${end - start}\n$ex")
    }
  }

  def writeTsv(wordcount: Map[String, Int]) = {
    import java.nio.file.{Paths, Files}
    import java.nio.charset.StandardCharsets

    val tsv = wordcount.toList.sortBy{ case (_, n) => n }.reverse.map{ case (s, n) => s"$s\t$n"}.mkString("\n")
    Files.write(Paths.get("words.tsv"), tsv.getBytes(StandardCharsets.UTF_8))
  }
}

// jury-rigged value store starting with `zero` and merging in new elements as provided
// could use an implicit Monoid, but then I'd need to pull in scalaz?
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
    b.foldLeft(a) { case (wc, (s, c)) =>
      wc.updated(s, c + wc.getOrElse(s, 0))
    }

  // in-memory single-operation update-only data stores backed by Akka Agents
  val wordcount: Store[Map[String, Int]] = new Store(Map.empty[String, Int])(merge)
  val commentcount: Store[Long] = new Store(0L)(_ + _)

  val subreddits = Vector("LifeProTips","explainlikeimfive","Jokes","askreddit", "funny", "news", "tifu")

  def main(args: Array[String]): Unit = {

    val links: Flow[Link] =
      Flow(subreddits) // start with a flow of subreddit names
        // for each subreddit name, get a Future[LinkListing] and fold the result into the stream
      .mapFuture( subreddit => RedditAPI.topLinks(subreddit) )
      .mapConcat( listing => listing.links) // concat a Flow of listings of links into a Flow of links

    // for each link, get a Future[CommentListing] and fold the result into the stream
    val comments: Flow[Comment] = links
      .mapFuture( link => RedditAPI.comments(link) )
      .mapConcat( listing => listing.comments) // concat a Flow of comment listings into a flow of comments


    val f: Future[Unit] = comments
      .groupedWithin(1000, 1 second) // group comments to avoid excessive IO
      //for each group, send an update off to the data stores and fold the resulting future(s) into the stream
      .mapFuture{ xs =>
        val updateCommentCount = commentcount.update(xs.length)
        val updateWordCount = wordcount.update(xs.map(_.toWordCount).reduce(merge))
        updateCommentCount zip updateWordCount
      }.foreach{ _ =>  } // use foreach here because it returns a future which completes when the stream is done.

    timedFuture("main stream")(f)
      .flatMap( _ => wordcount.read.zip(commentcount.read) ) // grab final word and comment count
      .onComplete{
        case Success((finalWordCount, cc)) =>
          writeTsv(finalWordCount)

          println(s"$cc comments")
          println(s"${finalWordCount.size} distinct words, ${finalWordCount.values.sum} words total")
          as.shutdown()
        case _ =>

          as.shutdown()
      }
  }

}