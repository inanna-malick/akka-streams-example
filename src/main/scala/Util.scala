package main

import scala.concurrent.duration._
import scala.concurrent._
import scala.util.{Success, Failure}
import scala.collection.immutable._
import akka.agent.Agent


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

  def writeTsv(wordcount: Map[String, Int]) = {
    import java.nio.file.{Paths, Files}
    import java.nio.charset.StandardCharsets

    val tsv = wordcount.toList.sortBy{ case (_, n) => n }.reverse.map{ case (s, n) => s"$s\t$n"}.mkString("\n")
    Files.write(Paths.get("words.tsv"), tsv.getBytes(StandardCharsets.UTF_8))
  }
}



// jury-rigged value store starting with `zero` and merging in new elements as provided
// could use an implicit Monoid, but then I'd need to pull in scalaz which is excessive
case class Store[T](zero: T)(f: (T, T) => T )(implicit val ec: ExecutionContext) {
  private val store: Agent[T] = Agent(zero)

  // add the wordcount to the current one
  def update(in: T): Future[Unit] =
    store.alter(f(_, in)).map( _ => () )

  // get the value of `store` after all queued updates have been completed
  def read: Future[T] =
    store.future()
}
