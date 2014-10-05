

import akka.actor.ActorSystem
import akka.agent.Agent
import akka.stream.{MaterializerSettings, FlowMaterializer}
import akka.stream.scaladsl.Flow
import org.json4s.JsonAST.{JValue, JString}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.immutable._

import dispatch._

object LinkListing {
  def fromJson(subreddit: String)(json: JValue) = {
    val x = json.\("data").\("children").children.map(_.\("data").\("id")).collect{ case JString(s) => Link(s, subreddit) }
    LinkListing(x)
  }
}
case class LinkListing(links: Seq[Link])
case class Link(id: String, subreddit: String)

object CommentListing {
  def fromJson(json: JValue) = {
    val x = json.\("data")
      .filterField{case ("body", _) => true; case _ => false }
      .collect{ case ("body", JString(s)) => Comment(s)}
    CommentListing(x)
  }
}
case class CommentListing(comments: Seq[Comment])
case class Comment(body: String)



object WordCount {
  implicit val as = ActorSystem()
  implicit val es = as.dispatcher
  implicit val mat = FlowMaterializer(MaterializerSettings())



  case object Tick

  type WordCount = Map[String, Long]

  val alpha = (('a' to 'z') ++ ('A' to 'Z')).toSet

  def normalize(s: Seq[String]): Seq[String] =
    s.map(_.filter(alpha.contains).map(_.toLower)).filterNot(_.isEmpty)

  def countWords(comment: Comment): WordCount =
    normalize(comment.body.split(" ").to[Seq])
      .groupBy(identity)
      .mapValues(_.length)


  def topLinks(subreddit: String): Future[LinkListing] = {
    //println(s"issue toplinks request for $subreddit")
    val page = url(s"http://www.reddit.com/r/$subreddit/top.json") <<? Map("t" -> "all")
    Http(page OK dispatch.as.json4s.Json).map(LinkListing.fromJson(subreddit)(_))
      .andThen{ case x => println(s"got ${x.map(_.links.length)} toplinks for $subreddit")}
  }

  def comments(link: Link): Future[CommentListing] = {
    //println(s"issue comments request for thread ${link.id} in ${link.subreddit}")
    val page = url(s"http://www.reddit.com/r/${link.subreddit}/comments/${link.id}.json") <<? Map("depth" -> "20", "limit" -> "2000")
    Http(page OK dispatch.as.json4s.Json).map(CommentListing.fromJson)
      .andThen{ case x => println(s"got ${x.map(_.comments.length)} comments for thread ${link.id} in ${link.subreddit}")}
  }

  def popularWords(w: WordCount): WordCount = {
    w.filter{ case (_,n) => n >= 200 }
  }



  def merge(a: WordCount, b: WordCount): WordCount =
    a.foldLeft(b){ case (wc, (s, c))  => wc.updated(s, c + wc.getOrElse(s, 0L)) }


  //issue all futures simultaneously, because mapfuture doesn't (may be a result of mat. settings)
  def mapConcatFuture[T, S](in: Flow[T])(f: T => Seq[Future[S]]): Flow[S] =
    in.mapFuture( t => Future.sequence(f(t)) ).mapConcat(identity)

  def buildStream(subreddits: Vector[String]): Flow[WordCount] = {
    Flow(subreddits) // Flow[String]
      .mapFuture(topLinks) // Flow[TopPosts]
    //mapConcatFuture(x)( ll => ll.links.map(comments)) //Flow[CommentListing]
      .mapConcat(_.links)
      .mapFuture(comments)
      .map( c => c.comments.map(countWords).fold(Map(): WordCount)(merge) ) // Flow[WordCount]
      .conflate(identity, merge) // Flow[WordCount]
  }

  def run(subreddits: Seq[String]) = {
    //todo: run 1 stream per subreddit, trivial parallel example
    ???
  }

  def consumeStream(interval: FiniteDuration)(in: Flow[WordCount]): Future[WordCount] = {
    val out: Agent[WordCount] = Agent(Map.empty[String, Long])

    val ticks = Flow(interval, interval, () => Tick)
    in.zip(ticks.toPublisher).foreach{ case (wordcount, _) =>
      println("send off update with total words: " + wordcount.values.sum)
      out.alter(merge(wordcount, _) ) //send update off. would've used redis but was too lazy
    }.map( _ => out.get() ) //foreach returns future[Unit],
  }

  def main(args: Array[String]): Unit = {

    val start = System.currentTimeMillis()

    val subreddits = Vector("LifeProTips","explainlikeimfive","Jokes","askreddit","tifu", "writingprompts", "Showerthoughts")

    consumeStream(5 seconds)(buildStream(subreddits)).onComplete{ x =>
      println(s"${x.map(_.size)} distinct words, ${x.map(_.values.sum)} words, most popular:\n${x.map(popularWords(_))}")
      val end = System.currentTimeMillis()
      println(s"total time elapsed: ${end - start}")
      as.shutdown()
    }
  }

}