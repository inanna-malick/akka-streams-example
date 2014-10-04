import akka.agent.Agent
import akka.stream.scaladsl.Flow

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration


case class Link(id: String)

case class LinkListing(links: Seq[Link])

case class CommentListing(comments: Seq[Comment])

case class Comment(body: String)

object WordCount {

  def topLinks(subreddit: String): Future[LinkListing] = ???

  def extractLinks(top: LinkListing): Seq[Link] = ???

  def comments(link: Link): Future[CommentListing] = ???

  def extractComments(comments: CommentListing): Seq[Comment]

  type WordCount = Map[String, Long]

  def countWords(comment: Comment): WordCount = ???

  def merge(a: WordCount)(b: WordCount): WordCount = ???

  def buildStream(subreddits: Vector[String]): Flow[WordCount] = {
    Flow(subreddits)
      .mapFuture(topLinks) // Flow[TopPosts]
      .mapConcat(extractLinks) // Flow[Link]
      .mapFuture(comments) // Flow[CommentListing]
      .mapConcat(extractComments) // Flow[Comment]
      .conflate(countWords, merge) // Flow[WordCount]
  }


  case object Tick

  def consumeStream(interval: FiniteDuration)(in: Flow[WordCount]): Future[WordCount] = {
    val out: Agent[WordCount] = Agent(Map.empty[String, Long])

    val ticks = Flow(interval, interval, () => Tick)
    in.zip(ticks.toPublisher).foreach{ case (wordcount, _) =>
      out.alter(merge(wordcount)) //send update off. would've used reddit but was too lazy
    }.map( _ => out.get() ) //foreach returns future[Unit],
  }

}
