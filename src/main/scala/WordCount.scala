package main

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{MaterializerSettings, FlowMaterializer}

import scala.collection.immutable.{Vector, Seq}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import Util._

import scala.util.{Failure, Success}


object WordCount {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  val settings = MaterializerSettings(as)
  implicit val mat = FlowMaterializer(settings)

  val store = new KVStore

  val redditAPIRate = 250 millis

  def merge(a: WordCount, b: WordCount): WordCount = {
    import scalaz._
    import Scalaz._

    a |+| b
  }


  //todo: single source of ticks, split between all zips...
  def throttled[T]: Flow[T, T] = {
    val tickSource = TickSource(redditAPIRate, redditAPIRate, () => () )
    val zip = Zip[T, Unit]
    val in = UndefinedSource[T]
    val out = UndefinedSink[T]
    PartialFlowGraph{ implicit builder =>
      import FlowGraphImplicits._
      in ~> zip.left  ~> Flow[(T,Unit)].map{ case (t, _) => t } ~> out
      tickSource ~> zip.right
    }.toFlow(in, out)
  }

  val fetchComments: Flow[Subreddit, Comment] =
    // 0) Create a duct that applies no transformations.
    Flow[Subreddit]
        // 1) Throttle the rate at which the next step can receive subreddit names.
        .via(throttled)
        // 2) Fetch links. Subject to rate limiting.
        .mapAsyncUnordered( subreddit => RedditAPI.popularLinks(subreddit) )
        // 3) Flatten a stream of link listings into a stream of links.
        .mapConcat( listing => listing.links )
        // 4) Throttle the rate at which the next step can receive links.
        .via(throttled)
        // 5) Fetch links. Subject to rate limiting.
        .mapAsyncUnordered( link => RedditAPI.popularComments(link) )
        // 6) Flatten a stream of comment listings into a stream of comments.
        .mapConcat( listing => listing.comments )

  val persistBatch: Flow[Comment, Int] =
    // 0) Create a duct that applies no transformations.
    Flow[Comment]
        // 1) Group comments, emitting a batch every 5000 elements
        //    or every 5 seconds, whichever comes first.
        .groupedWithin(5000, 5 second)
        // 2) Group comments by subreddit and write the wordcount
        //    for each group to the store. This step outputs
        //    the size of each batch after it is persisted.
        .mapAsyncUnordered{ batch =>
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
    val subreddits: Source[Subreddit] =
      if (args.isEmpty)
        Source(RedditAPI.popularSubreddits).mapConcat(identity)
      else
        Source(args.toVector)

    // 1) Append ducts to the initial flow and materialize it via forEach.
    //    The resulting future succeeds if stream processing completes
    //    or fails if an error occurs.
    val streamF: Future[Unit] =
      subreddits
        .via(fetchComments)
        .via(persistBatch)
        .foreach{ n => println(s"persisted $n comments")}

    // 2) When stream processing is finished, load the resulting
    //    word counts from the store, log some basic statistics,
    //    and write them to a .tsv files (code omited for brevity)
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
