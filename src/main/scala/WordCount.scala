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

  /** transform a stream of subreddit names into a stream of the top comments posted on the top links in that subreddit
   */
  def fetchComments: Duct[Subreddit, Comment] = 
    // 0) create a duct that applies no transformations
    Duct[Subreddit] 
        // 1) throttle the rate at which the next step can receive subreddit names
        .zip(throttle.toPublisher).map{ case (t, Tick) => t } 
        // 2) fetch links. Subject to rate limiting
        .mapFuture( subreddit => RedditAPI.popularLinks(subreddit) ) 
        // 3) flatten a stream of link listings into a stream of links
        .mapConcat( listing => listing.links ) 
        // 4) throttle the rate at which the next step can receive links
        .zip(throttle.toPublisher).map{ case (t, Tick) => t } 
        // 5) fetch links. Subject to rate limiting
        .mapFuture( link => RedditAPI.popularComments(link) ) 
        // 6) flatten a stream of comment listings into a stream of comments
        .mapConcat( listing => listing.comments ) 

  /** this Duct takes a stream of comments and, in batches of 2000 or every 5 seconds, 
   * whichever comes first, writes them to the store. 
   * It outputs the size of each batch persisted
   */
  val persistBatch: Duct[Comment, Int] = 
    Duct[Comment]  // first, create a duct that doesn't apply any transformations
        .groupedWithin(2000, 5 second) // group comments to avoid excessive IO. Emits Seq[Comment]'s every 2000 elements or 5 seconds, whichever comes first
        .mapFuture{ batch => 
          val grouped: Map[Subreddit, WordCount] = batch
            .groupBy(_.subreddit) // group each batch of comments by subreddit
            .mapValues(_.map(_.toWordCount).reduce(merge)) // convert each group of comments into word counts and merge them into a single word count
          val fs = grouped.map{ case (subreddit, wordcount) => store.addWords(subreddit, wordcount) } // write each (subreddit, wordcount) pair to the store
          Future.sequence(fs).map{ _ => batch.size } // create a future that will complete when all store operations complete, holding the size of this batch
        }

  def main(args: Array[String]): Unit = {
    val subreddits: Flow[Subreddit] =  // create the initial Flow of Subreddits from either the cmd line input or reddit's popular subreddit api call
      if (args.isEmpty) Flow(RedditAPI.popularSubreddits).mapConcat(identity) //intialize a flow from the result of a Future
      else Flow(args.toVector) //initialize a flow from a vector

    // append ducts to the initial flow and materialize it using forEach
    val streamF: Future[Unit] = 
      subreddits
        .append(fetchComments)
        .append(persistBatch)
        .foreach{ n => println(s"persisted $n comments")}

    // when the stream completes, write the contents of the store to per-subreddit .tsv files
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
