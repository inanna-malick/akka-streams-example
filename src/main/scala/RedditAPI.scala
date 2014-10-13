package main

import dispatch._
import Util.timedFuture
import scala.concurrent.duration._
import scala.concurrent.{Future, ExecutionContext}
import org.json4s.JsonAST.{JValue, JString}
import scala.collection.immutable._

import Util._

object RedditAPI {
  val useragent = Map("User-Agent" -> "wordcloud mcgee")

  def popularLinks(subreddit: Subreddit)(implicit ec: ExecutionContext): Future[LinkListing] = 
    withRetry(timedFuture(s"links: r/$subreddit/top"){
      val page = url(s"http://www.reddit.com/r/$subreddit/top.json") <<? Map("limit" -> "50", "t" -> "all") <:< useragent
      Http(page OK dispatch.as.json4s.Json).map(LinkListing.fromJson(subreddit)(_))
    }, LinkListing(Seq.empty))

  def popularComments(link: Link)(implicit ec: ExecutionContext): Future[CommentListing] = 
    withRetry(timedFuture(s"comments: r/${link.subreddit}/${link.id}/comments"){
      val page = url(s"http://www.reddit.com/r/${link.subreddit}/comments/${link.id}.json") <<? Map("depth" -> "25", "limit" -> "2000") <:< useragent
      Http(page OK dispatch.as.json4s.Json).map(json => CommentListing.fromJson(json, link.subreddit))
    }, CommentListing(link.subreddit, Seq.empty))

  def popularSubreddits(implicit ec: ExecutionContext): Future[Seq[Subreddit]] = 
    timedFuture("fetch popular subreddits"){
      val page = url(s"http://www.reddit.com/subreddits/popular.json").GET <<? Map("limit" -> "50") <:< useragent
      Http(page OK dispatch.as.json4s.Json).map{ json =>
        json.\("data").\("children").children
          .map(_.\("data").\("url"))
          .collect{ case JString(url) => url.substring(3, url.length - 1) }
          .map{ x => println(x); x}
      }
    }
}


object LinkListing {
  def fromJson(subreddit: Subreddit)(json: JValue) = {
    val x = json.\("data").\("children").children.map(_.\("data").\("id")).collect{ case JString(s) => Link(s, subreddit) }
    LinkListing(x)
  }
}
case class LinkListing(links: Seq[Link])
case class Link(id: String, subreddit: Subreddit)

object CommentListing {
  def fromJson(json: JValue, subreddit: Subreddit) = {
    val x = json.\("data")
      .filterField{case ("body", _) => true; case _ => false }
      .collect{ case ("body", JString(s)) => Comment(subreddit, s)}
    CommentListing(subreddit, x)
  }
}
case class CommentListing(subreddit: Subreddit, comments: Seq[Comment])
case class Comment(subreddit: Subreddit, body: String){
  val alpha = (('a' to 'z') ++ ('A' to 'Z')).toSet

  def normalize(s: Seq[String]): Seq[String] =
    s.map(_.filter(alpha.contains).map(_.toLower)).filterNot(_.isEmpty)

  def toWordCount: WordCount =
    normalize(body.split(" ").to[Seq])
      .groupBy(identity)
      .mapValues(_.length)
}


