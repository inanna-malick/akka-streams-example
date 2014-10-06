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

object WordCount {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  val settings = MaterializerSettings().withFanOut(16, 32).withBuffer(16, 32)
  implicit val mat = FlowMaterializer(settings)
  println(s"settings: $settings")

  //todo: mergeable datastructure
  def merge(a: Map[String, Int], b: Map[String, Int]): Map[String, Int] =
    a.foldLeft(b) { case (wc, (s, c)) => wc.updated(s, c + wc.getOrElse(s, 0))}

  val subreddits = Vector("LifeProTips","explainlikeimfive","Jokes","askreddit", "funny", "news", "tifu")

  def main(args: Array[String]): Unit = {
    // agent, accepts sequence of functions T => T, runs them in sequence.
    // todo: abstract this out into a mockDB class w/ all interactions via Future to make example more concrete
    val out = Agent(Map.empty[String, Int])

    val f: Future[Unit] =
      Flow(subreddits) // start with a flow of subreddit names
      .mapFuture(RedditAPI.topLinks) // for each subreddit name, get a Future[LinkListing] and fold the result into the stream
      .mapConcat(_.links) // concat a Flow of listings of links into a Flow of links
      .mapFuture(RedditAPI.comments) // for each link, get a Future[CommentListing] and fold the result into the stream
      .mapConcat(_.comments) // concat a Flow of comment listings into a flow of comments
      .map(_.words) // get a wordcount for each
      .foreach{ wordcount => // fold each wordcount into the result-holder agent
        // since we're just performing addition for each of N keys order is irrelevant, which is a nice bonus
        // I'm using an agent here to keep this example in-memory.
      out.send(merge(wordcount, _))
     }

    timedFuture("main stream")(f).onComplete{
      case Success(_) =>
        //grab and log final agent state
        val res = out.get()
        println(s"${res.size} distinct words, ${res.values.sum} words total")
        as.shutdown()
      case _ =>
        as.shutdown()
    }
  }
}