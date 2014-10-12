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

import java.io.File

// todo: per/reddit store, per-reddit output TSV.
object WordCount {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  val settings = MaterializerSettings()//.withFanOut(16, 32).withBuffer(16, 32)
  implicit val mat = FlowMaterializer(settings)


  val defaultSubreddits: Vector[Subreddit] = Vector("LifeProTips","explainlikeimfive","Jokes","askreddit", "funny", "news")

  val store = new KVStore

  def merge(a: WordCount, b: WordCount): WordCount = a |+| b

  val fetchComments: Duct[Subreddit, Comment] = 
    Duct[Subreddit]
        .mapFuture( subreddit => RedditAPI.topLinks(subreddit) )
        .mapConcat( listing => listing.links )
        .mapFuture( link => RedditAPI.comments(link) )
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
    val subreddits: Vector[String] = 
      if (args.isEmpty) defaultSubreddits
      else args.toVector

    val streamF: Future[Unit] = 
      Flow(subreddits)
        .append(fetchComments)
        .append(persistBatch)
        .foreach{ n => println(s"persisted $n comments")}

    timedFuture("main stream")(streamF)
      .flatMap( _ => store.wordCounts)
      .onComplete{
        case Success(wordcounts) =>
          for {
            files <- Option(new File("res").listFiles)
            file <- files if file.getName.endsWith(".tsv")
          } file.delete()

          wordcounts.foreach{ case (subreddit, wordcount) =>
            val fname = s"res/$subreddit.tsv"
            println(s"write wordcount for $subreddit to $fname")
            writeTsv(fname, wordcount)
          }
          as.shutdown()
        case Failure(err) =>
          println(s"stream finished with error: $err") 
          as.shutdown()
      }
  }
}
