package main

import scala.concurrent.duration._
import scala.concurrent._
import scala.util.{Success, Failure}
import scala.collection.immutable._
import akka.agent.Agent
import scalaz._
import Scalaz._
import Util._


object Util {
  def timedFuture[T](name: String)(f: Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = System.currentTimeMillis()
    println(s"--> started $name")
    f.andThen{
      case Success(_) =>
        val end = System.currentTimeMillis()
        println(s"\t<-- finished $name, total time elapsed: ${end - start}")
      case Failure(ex) =>
        val end = System.currentTimeMillis()
        println(s"\t<X> failed $name, total time elapsed: ${end - start}\n$ex")
    }
  }

  def withRetry[T](f: => Future[T], onFail: T, n: Int = 3)(implicit ec: ExecutionContext): Future[T] = 
    if (n > 0){ f.recoverWith{ case err: Exception => 
      println(s"future failed with $err, retrying")
      withRetry(f, onFail, n - 1)
    }} else{
      println(s"WARNING: failed to run future, substituting $onFail")
      Future.successful(onFail)
    }

  def writeTsv(fname: String, wordcount: WordCount) = {
    import java.nio.file.{Paths, Files}
    import java.nio.charset.StandardCharsets

    val tsv = wordcount.toList.sortBy{ case (_, n) => n }.reverse.map{ case (s, n) => s"$s\t$n"}.mkString("\n")
    Files.write(Paths.get(fname), tsv.getBytes(StandardCharsets.UTF_8))
  }

  type WordCount = Map[String, Int]
  type Subreddit = String  
}


class KVStore(implicit val ec: ExecutionContext) {
  private val commentCount: Store[Map[Subreddit,WordCount]] = new Store

  def addWords(subreddit: Subreddit, words: WordCount): Future[Unit] = {
    commentCount.update(Map(subreddit -> words))
  }

  def wordCounts: Future[Map[Subreddit, WordCount]] = commentCount.read
}

// jury-rigged value store
class Store[T](implicit val m: Monoid[T], implicit val ec: ExecutionContext) {
  private val store: Agent[T] = Agent(m.zero)
  
  def update(in: T): Future[Unit] =
    store.alter(m.append(_, in)).map( _ => () )

  def read: Future[T] =
    store.future()
}
