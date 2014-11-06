package main

import akka.actor.ActorSystem
import akka.stream.{MaterializerSettings, FlowMaterializer}
import akka.stream.scaladsl.{Flow, Duct}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.immutable.{Vector, Seq}
import Util._

import scala.io.Source
import scala.util.{Failure, Success}

import scalaz._
import Scalaz._


object WordCount {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  val settings = MaterializerSettings(as)
  implicit val mat = FlowMaterializer(settings)

  val store = new KVStore

  val redditAPIRate = 250 millis

  case object Tick
  val throttle = Flow(redditAPIRate, redditAPIRate, () => Tick)
  
  def merge(a: WordCount, b: WordCount): WordCount = a |+| b

  def fetchComments: Duct[Subreddit, Comment] = 
    // 0) Create a duct that applies no transformations.
    Duct[Subreddit] 
        // 1) Throttle the rate at which the next step can receive subreddit names.
        .zip(throttle.toPublisher).map{ case (t, Tick) => t } 
        // 2) Fetch links. Subject to rate limiting.
        .mapFuture( subreddit => RedditAPI.popularLinks(subreddit) ) 
        // 3) Flatten a stream of link listings into a stream of links.
        .mapConcat( listing => listing.links ) 
        // 4) Throttle the rate at which the next step can receive links.
        .zip(throttle.toPublisher).map{ case (t, Tick) => t } 
        // 5) Fetch links. Subject to rate limiting.
        .mapFuture( link => RedditAPI.popularComments(link) ) 
        // 6) Flatten a stream of comment listings into a stream of comments.
        .mapConcat( listing => listing.comments )

  val persistBatch: Duct[Comment, Int] = 
    // 0) Create a duct that applies no transformations.
    Duct[Comment]
        // 1) Group comments, emitting a batch every 5000 elements
        //    or every 5 seconds, whichever comes first.
        .groupedWithin(5000, 5 second) 
        // 2) Group comments by subreddit and write the wordcount 
        //    for each group to the store. This step outputs 
        //    the size of each batch after it is persisted.
        .mapFuture{ batch => 
          val grouped: Map[Subreddit, WordCount] = batch
            .groupBy(_.subreddit)
            .mapValues(_.map(_.toWordCount).reduce(merge))
          val fs = grouped.map{ case (subreddit, wordcount) => 
              store.addWords(subreddit, wordcount)
            }
          Future.sequence(fs).map{ _ => batch.size }
        }

  def main(args: Array[String]): Unit = {
    // 0) Create a Flow of Subreddit names, using either
    //    the argument vector or the result of an API call.
    val subreddits: Flow[Subreddit] =
      if (args.isEmpty) 
        Flow(RedditAPI.popularSubreddits).mapConcat(identity)
      else
        Flow(args.toVector)

    // 1) Append ducts to the initial flow and materialize it via forEach. 
    //    The resulting future succeeds if stream processing completes 
    //    or fails if an error occurs.
    val streamF: Future[Unit] = 
      subreddits
        .append(fetchComments)
        .append(persistBatch)
        .foreach{ n => println(s"persisted $n comments")}

    // 2) When stream processing is finished, load the resulting wordcounts from the store, 
    //    log some basic statisitics, and write them to a .tsv files
    timedFuture("main stream")(streamF)
      .flatMap( _ => store.wordCounts)
      .onComplete{
        case Success(wordcounts) =>
          clearOutputDir()
          wordcounts.foreach{ case (subreddit, wordcount) =>
            val fname = s"res/$subreddit.tsv"
            println(s"write wordcount for $subreddit to $fname")
            writeTsv(fname, wordcount)
            println(s"${wordcount.size} discrete words and ${wordcount.values.sum} total words for $subreddit")
          }
          as.shutdown()
        case Failure(err) =>
          println(s"stream finished with error: $err") 
          as.shutdown()
      }
  }
}
