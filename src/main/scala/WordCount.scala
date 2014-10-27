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


// todo: per/reddit store, per-reddit output TSV.
object WordCount {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  val settings = MaterializerSettings()
  implicit val mat = FlowMaterializer(settings)

  val defaultSubreddits: Vector[Subreddit] = Vector("LifeProTips","explainlikeimfive","Jokes","askreddit", "funny", "news")

  val store = new KVStore


  val redditAPIRate = 250 millis

  case object Tick
  val throttle = Flow(redditAPIRate, redditAPIRate, () => Tick)
  
  def merge(a: WordCount, b: WordCount): WordCount = a |+| b

  /** transforms a stream of subreddits into a stream of the top comments
   *  posted in each of the top threads in that subreddit
   */
  def fetchComments: Duct[Subreddit, Comment] = 
    Duct[Subreddit]
        .zip(throttle.toPublisher).map{ case (t, Tick) => t } 
        .mapFuture( subreddit => RedditAPI.popularLinks(subreddit) )
        .mapConcat( listing => listing.links )
        .zip(throttle.toPublisher).map{ case (t, Tick) => t }
        .mapFuture( link => RedditAPI.popularComments(link) )
        .mapConcat( listing => listing.comments )

  val persistBatch: Duct[Comment, Int] = 
    Duct[Comment]
        .groupedWithin(1000, 5 second) // group comments to avoid excessive IO
        .mapFuture{ batch => 
          val grouped: Map[Subreddit, WordCount] = batch
            .groupBy(_.subreddit)
            .mapValues(_.map(_.toWordCount).reduce(merge))
          val fs = grouped.map{ case (subreddit, wordcount) => store.addWords(subreddit, wordcount) }
          Future.sequence(fs).map{ _ => batch.size }
        }

  def main(args: Array[String]): Unit = {
    val subreddits: Flow[String] = 
      if (args.isEmpty) Flow(RedditAPI.popularSubreddits).mapConcat(identity)
      else Flow(args.toVector)

    val streamF: Future[Unit] = 
      subreddits
        .append(fetchComments)
        .append(persistBatch)
        .foreach{ n => println(s"persisted $n comments")}

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
