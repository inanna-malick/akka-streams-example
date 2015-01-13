package main

import scala.concurrent.duration._
import scala.concurrent._
import scala.util.{Success, Failure}
import scala.collection.immutable._
import akka.stream._
import akka.stream._
import akka.actor._
import akka.stream.actor._
import scalaz._
import Scalaz._
import Util._
import java.io.File
import java.nio.file.{Paths, Files}
import org.reactivestreams.Subscriber
import java.nio.charset.StandardCharsets


object Util {
  def timedFuture[T](name: String)(f: Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = System.currentTimeMillis()
    println(s"--> started $name")
    f.andThen{
      case Success(t) =>
        val end = System.currentTimeMillis()
        println(s"\t<-- finished $name after ${end - start} millis")
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
    val tsv = wordcount.toList.sortBy{ case (_, n) => n }.reverse.map{ case (s, n) => s"$s\t$n"}.mkString("\n")
    Files.write(Paths.get(fname), tsv.getBytes(StandardCharsets.UTF_8))
  }

  def clearOutputDir() =
    for {
      files <- Option(new File("res").listFiles)
      file <- files if file.getName.endsWith(".tsv")
    } file.delete()

  type WordCount = Map[String, Int]

  def mergeWordCounts(a: Map[String, WordCount], b: Map[String, WordCount]) = a |+| b

  def writeResults(wordcounts: Map[String, WordCount]) = {
    clearOutputDir()
    wordcounts.foreach{ case (key, wordcount) =>
      val fname = s"res/$key.tsv"
      println(s"write wordcount for $key to $fname")
      writeTsv(fname, wordcount)
      println(s"${wordcount.size} distinct words and ${wordcount.values.sum} total words for $key")
    }
  }
}
