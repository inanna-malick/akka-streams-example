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


// todo: per/reddit store, per-reddit output TSV.
object WordCount {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  val settings = MaterializerSettings().withFanOut(16, 32).withBuffer(16, 32)
  implicit val mat = FlowMaterializer(settings)

  type WordCount = Map[String, Int]

  def merge(a: WordCount, b: WordCount): WordCount =
    b.foldLeft(a) { case (wc, (s, c)) =>
      wc.updated(s, c + wc.getOrElse(s, 0))
    }

  // in-memory single-operation update-only data stores backed by Akka Agents
  val wordcount: Store[WordCount] = new Store(Map.empty[String, Int])(merge)
  val commentcount: Store[Long] = new Store(0L)(_ + _)


  def persistComments(batch: Seq[Comment]): Future[Unit] = {
    val updateCommentCount = commentcount.update(batch.length)
    val updateWordCount = wordcount.update(batch.map(_.toWordCount).reduce(merge))
    updateCommentCount.zip(updateWordCount).map{ _ => }
  }


  type Subreddit = String
  val defaultSubreddits: Vector[Subreddit] = Vector("LifeProTips","explainlikeimfive","Jokes","askreddit", "funny", "news")

  val topLinks: Duct[Subreddit, Link] = 
    Duct[Subreddit]
        .mapFuture( subreddit => RedditAPI.topLinks(subreddit) )
        .mapConcat( listing => listing.links )

  val topComments: Duct[Link, Comment] = 
    Duct[Link]
        .mapFuture( link => RedditAPI.comments(link) )
        .mapConcat( listing => listing.comments )

  val persistBatch: Duct[Comment, Int] = 
    Duct[Comment]
        .groupedWithin(1000, 1 second) // group comments to avoid excessive IO
        .mapFuture( batch => persistComments(batch).map( _ => batch.length ) // send batches to data store

  def main(args: Array[String]): Unit = {
    val subreddits: Vector[String] = 
      if (args.isEmpty) defaultSubreddits
      else              args.toVector


    val streamF: Future[Unit] = 
      Flow(subreddits)
        .append(topLinks)
        .append(topComments)
        .append(persistBatch)
        .foreach{ n => println(s"persisted batch of size $n")}

    timedFuture("main stream")(streamF)
      .flatMap( _ => wordcount.read.zip(commentcount.read) ) // grab final word and comment count
      .onComplete{
        case Success((finalWordCount, cc)) =>
          writeTsv(finalWordCount)

          println(s"$cc comments")
          println(s"${finalWordCount.size} distinct words, ${finalWordCount.values.sum} words total")
          as.shutdown()
        case _ =>

          as.shutdown()
      }
  }

}
