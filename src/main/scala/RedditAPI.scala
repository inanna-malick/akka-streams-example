package com.pkinsky

import dispatch._
import scala.concurrent.duration._
import scala.concurrent.{Future, ExecutionContext}
import org.json4s.JsonAST.{JValue, JString}
import scala.collection.immutable._

object RedditAPI {

  val linksToFetch = 15
  val subredditsToFetch = 5
  val commentsToFetch = 2000
  val commentDepth = 25

  val useragent = Map("User-Agent" -> "wordcloud mcgee")

  def popularLinks(subreddit: String)(implicit ec: ExecutionContext): Future[LinkListing] = 
    withRetry(timedFuture(s"links: r/$subreddit/top"){
      val page = url(s"http://www.reddit.com/r/$subreddit/top.json") <<? Map("limit" -> linksToFetch.toString, "t" -> "all") <:< useragent
      Http(page OK dispatch.as.json4s.Json).map(LinkListing.fromJson(subreddit)(_))
    }, LinkListing(Seq.empty))

  def popularComments(link: Link)(implicit ec: ExecutionContext): Future[CommentListing] = 
    withRetry(timedFuture(s"comments: r/${link.subreddit}/${link.id}/comments"){
      val page = url(s"http://www.reddit.com/r/${link.subreddit}/comments/${link.id}.json") <<? Map("depth" -> commentDepth.toString, "limit" -> commentsToFetch.toString) <:< useragent
      Http(page OK dispatch.as.json4s.Json).map(json => CommentListing.fromJson(json, link.subreddit))
    }, CommentListing(link.subreddit, Seq.empty))


  def popularSubreddits(implicit ec: ExecutionContext): Future[Seq[String]] = 
    timedFuture("fetch popular subreddits"){
      val page = url(s"http://www.reddit.com/subreddits/popular.json").GET <<? Map("limit" -> subredditsToFetch.toString) <:< useragent
      Http(page OK dispatch.as.json4s.Json).map{ json =>
        json.\("data").\("children").children
          .map(_.\("data").\("url"))
          .collect{ case JString(url) => url.substring(3, url.length - 1) }
        }
    }
}

object SimpleExample {
  import RedditAPI._
  import ExecutionContext.Implicits.global

  def run =
    for {
      subreddits <- popularSubreddits
      linklistings <- Future.sequence(subreddits.map(popularLinks))
      links = linklistings.flatMap(_.links)
      commentListings <- Future.sequence(links.map(popularComments))
      comments = commentListings.flatMap(_.comments)
    } yield comments
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

  def toWordCount: WordCount =
    normalize(body.split(" ").to[Seq])
      .groupBy(identity)
      .mapValues(_.length)
}


