package com.pkinsky

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream._
import akka.stream.actor._
import scala.language.postfixOps
import scala.collection.immutable.{Vector, Seq}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import scala.util.{Failure, Success}


object Main {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  val settings = ActorFlowMaterializerSettings(as)
  implicit val mat = ActorFlowMaterializer(settings)

  val redditAPIRate = 500 millis

  /**
    builds the following stream-processing graph.
    +------------+
    | tickSource +-Unit-+
    +------------+      +---> +-----+            +-----+      +-----+
                              | zip +-(T,Unit)-> | map +--T-> | out |
    +----+              +---> +-----+            +-----+      +-----+
    | in +----T---------+
    +----+
    tickSource emits one element per `rate` time units and zip only emits when an element is present from its left and right
    input stream, so the resulting stream can never emit more than 1 element per `rate` time units.
   */
  def throttle[T](rate: FiniteDuration): Flow[T, T] = {
    val tickSource = TickSource(rate, rate, () )
    val zip = Zip[T, Unit]
    val in = UndefinedSource[T]
    val out = UndefinedSink[(T, Unit)]
    PartialFlowGraph{ implicit builder =>
      import FlowGraphImplicits._
      in ~> zip.left
      tickSource ~> zip.right
      zip.out ~> out
    }.toFlow(in, out).map{ case (t, _) => t }
  }

  val fetchLinks: Flow[String, Link] =
    Flow[String]
        .via(throttle(redditAPIRate))
        .mapAsyncUnordered( subreddit => RedditAPI.popularLinks(subreddit) )
        .mapConcat( listing => listing.links )


  val fetchComments: Flow[Link, Comment] =
    Flow[Link]
        .via(throttle(redditAPIRate))
        .mapAsyncUnordered( link => RedditAPI.popularComments(link) )
        .mapConcat( listing => listing.comments )

  val wordCountSink: FoldSink[Map[String, WordCount], Comment] =
    FoldSink(Map.empty[String, WordCount])( 
      (acc: Map[String, WordCount], c: Comment) => 
        mergeWordCounts(acc, Map(c.subreddit -> c.toWordCount))
    )

def main(args: Array[String]): Unit = {
    // 0) Create a Flow of String names, using either
    //    the argument vector or the result of an API call.
    val subreddits: Source[String] =
      if (args.isEmpty)
        Source(RedditAPI.popularSubreddits).mapConcat(identity)
      else
        Source(args.toVector)
  
    val res: Future[Map[String, WordCount]] =
      subreddits
      .via(fetchLinks)
      .via(fetchComments)
      .runWith(wordCountSink)

    res.onComplete(writeResults)

    as.awaitTermination()
  }
}
