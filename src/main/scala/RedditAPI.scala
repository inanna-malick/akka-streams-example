package main

import dispatch._
import Util.timedFuture
import scala.concurrent.duration._
import scala.concurrent.{Future, ExecutionContext}
import org.json4s.JsonAST.{JValue, JString}
import scala.collection.immutable._



object RedditAPI {
  def topLinks(subreddit: String)(implicit ec: ExecutionContext): Future[LinkListing] = {
    val page = url(s"http://www.reddit.com/r/$subreddit/top.json") <<? Map("limit" -> "25", "t" -> "all")
    val f = Http(page OK dispatch.as.json4s.Json).map(LinkListing.fromJson(subreddit)(_))
    timedFuture(s"links: r/$subreddit/top")(f)
  }

  def comments(link: Link)(implicit ec: ExecutionContext): Future[CommentListing] = {
    val page = url(s"http://www.reddit.com/r/${link.subreddit}/comments/${link.id}.json") <<? Map("depth" -> "25", "limit" -> "2000")
    val f = Http(page OK dispatch.as.json4s.Json).map(json => CommentListing.fromJson(json, link.subreddit))
    timedFuture(s"comments: r/${link.subreddit}/${link.id}/comments")(f)
  }
}


object LinkListing {
  def fromJson(subreddit: String)(json: JValue) = {
    val x = json.\("data").\("children").children.map(_.\("data").\("id")).collect{ case JString(s) => Link(s, subreddit) }
    LinkListing(x)
  }
}
case class LinkListing(links: Seq[Link])
case class Link(id: String, subreddit: String)

object CommentListing {
  def fromJson(json: JValue, subreddit: String) = {
    val x = json.\("data")
      .filterField{case ("body", _) => true; case _ => false }
      .collect{ case ("body", JString(s)) => Comment(subreddit, s)}
    CommentListing(subreddit, x)
  }
}
case class CommentListing(subreddit: String, comments: Seq[Comment])
case class Comment(subreddit: String, body: String){
  val alpha = (('a' to 'z') ++ ('A' to 'Z')).toSet

  def normalize(s: Seq[String]): Seq[String] =
    s.map(_.filter(alpha.contains).map(_.toLower)).filterNot(_.isEmpty)

  def toWordCount: Map[String, Int] =
    normalize(body.split(" ").to[Seq])
      .groupBy(identity)
      .mapValues(_.length)
}


