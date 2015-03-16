package com.pkinsky

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream._
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.Future


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
  def throttle[T](rate: FiniteDuration): Flow[T, T, Unit] = {
    Flow() { implicit builder =>
      import akka.stream.scaladsl.FlowGraph.Implicits._
      val zip = builder.add(Zip[T, Unit.type]())
      Source(rate, rate, Unit) ~> zip.in1
      (zip.in0, zip.out)
    }.map(_._1)
  }

  val fetchLinks: Flow[String, Link, Unit] =
    Flow[String]
        .via(throttle(redditAPIRate))
        .mapAsyncUnordered( subreddit => RedditAPI.popularLinks(subreddit) )
        .mapConcat( listing => listing.links )


  val fetchComments: Flow[Link, Comment, Unit] =
    Flow[Link]
        .via(throttle(redditAPIRate))
        .mapAsyncUnordered( link => RedditAPI.popularComments(link) )
        .mapConcat( listing => listing.comments )

  val wordCountSink: Sink[Comment, Future[Map[String, WordCount]]] =
    Sink.fold(Map.empty[String, WordCount])(
      (acc: Map[String, WordCount], c: Comment) => 
        mergeWordCounts(acc, Map(c.subreddit -> c.toWordCount))
    )

def main(args: Array[String]): Unit = {
    // 0) Create a Flow of String names, using either
    //    the argument vector or the result of an API call.
    val subreddits: Source[String, Unit] =
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
