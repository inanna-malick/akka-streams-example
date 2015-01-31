package com

import scala.concurrent.duration._
import scala.concurrent._
import scala.util.{Success, Failure, Try}
import scala.collection.immutable._
import akka.stream._
import akka.stream._
import akka.actor._
import akka.stream.actor._
import scalaz._
import Scalaz._
import java.io.File
import java.nio.file.{Paths, Files}
import org.reactivestreams.Subscriber
import java.nio.charset.StandardCharsets


package object pkinsky {

  private val tZero = System.currentTimeMillis()

  private val errorColor = Console.RED
  private val successColor = Console.CYAN

  def printlnC(s: String): Unit = println(Console.CYAN + s + Console.RESET)

  def timedFuture[T](name: String)(f: Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = System.currentTimeMillis()
    println(s"${Console.CYAN}--> started $name at t0 + ${start - tZero}${Console.RESET}")
    f.andThen{
      case Success(t) =>
        val end = System.currentTimeMillis()
        println(s"\t${Console.CYAN}<-- finished $name after ${end - start} millis${Console.RESET}")
      case Failure(ex) =>
        val end = System.currentTimeMillis()
        println(s"\t${Console.RED}<X> failed $name, total time elapsed: ${end - start}\n$ex${Console.RESET}")
    }
  }

  def withRetry[T](f: => Future[T], onFail: T, n: Int = 3)(implicit ec: ExecutionContext): Future[T] = 
    if (n > 0){ f.recoverWith{ case err: Exception => 
      println(s"${Console.RED}future failed with $err, retrying${Console.RESET}")
      withRetry(f, onFail, n - 1)
    }} else{
      println(s"${Console.RED}WARNING: failed to run future, substituting $onFail${Console.RESET}")
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

  def writeResults(wordcounts: Try[Map[String, WordCount]])(implicit as: ActorSystem) = wordcounts match {
    case Success(wordcounts) =>
      clearOutputDir()
      wordcounts.foreach{ case (key, wordcount) =>
        val fname = s"res/$key.tsv"
        println(s"${Console.CYAN}write wordcount for $key to $fname${Console.RESET}")
        writeTsv(fname, wordcount)
        println(s"${Console.CYAN}${wordcount.size} distinct words and ${wordcount.values.sum} total words for $key${Console.RESET}")
      }
      as.shutdown()

    case Failure(f) => 
      println(s"${Console.RED}failed with $f${Console.RESET}")
      as.shutdown()
  }
}
