import akka.actor.{Props, ActorSystem, ActorRef, Actor}
import akka.stream.{MaterializerSettings, FlowMaterializer}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.Flow
import twitter4j._

import scala.collection.immutable.Queue

import scala.concurrent.duration._

object StatusStreamer {

  implicit val as = ActorSystem()
  implicit val es = as.dispatcher
  implicit val mat = FlowMaterializer(MaterializerSettings())

  val listener = as.actorOf(Props(new TwitterListener))

  def main(args: Array[String]) {


    Flow(Vector(1,2)).fold(0)(_ + _).foreach{
      x => println("foldres: " + x)
    }

    /*
    val bostonBoundingBox = Array(Array(-71.19,42.28), Array(-70.92,42.44))

    val twitterStream = new TwitterStreamFactory(Util.config).getInstance
    twitterStream.addListener(Util.statusPublisher(listener))
    twitterStream.sample
      //filter(new FilterQuery().locations(bostonBoundingBox))


    buildStream

    Thread.sleep(20000)
    twitterStream.cleanUp
    twitterStream.shutdown
    as.shutdown()*/
  }

  def buildStream = {
    Flow(ActorPublisher[Status](listener))
    .groupBy(status => status.getUser.getLang).foreach{ case (lang, stream) =>
      Flow(stream).groupedWithin(5, 1 second).foreach{ statuses =>
        val text = statuses.map(_.getText).map(t => s"\t- $t").mkString(",\n")
        println(s"statuses for $lang:\n$text")

      }.onComplete{ case x =>
        println(s"grouped flow for $lang completed with $x")
      }

    }.onComplete{ case x =>
      println(s"twitter status flow completed with $x")
    }


  }

}

object Util {

  val config = new twitter4j.conf.ConfigurationBuilder()
    .setOAuthConsumerKey("x1Idz9s46pp2IhPlRvuRqJ8Aw")
    .setOAuthConsumerSecret("epBVKt6InzPRBAlivW5rUascxoR9bARdXwPNAtlHNbZoHZW9BW")
    .setOAuthAccessToken("287184001-EUTy4WBiLSPTZmRHa8mRw7lOdOjdt2JIiIR0Ovxt")
    .setOAuthAccessTokenSecret("JLYMsHHw5SDwzCEfxo2hq3xGPyZNyXcXnAhdUpQt2lJGl")
    .build



  def statusPublisher(publisher: ActorRef) = new StatusListener() {
    def onStatus(status: Status) { publisher ! StatusUpdate(status) }
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
    def onException(ex: Exception) { ex.printStackTrace()}
    def onScrubGeo(arg0: Long, arg1: Long) {}
    def onStallWarning(warning: StallWarning) {}
  }

}

case object Cancel
case class StatusUpdate(status: Status)

class TwitterListener extends Actor with ActorPublisher[Status] {

  var q = Queue.empty[Status]

  def receive = {
    case StatusUpdate(status) =>
      if (totalDemand > 0) onNext(status)
      else q = q.enqueue(status)

    case Request(n) =>
      val (toSend, toKeep) = q.splitAt(n)
      toSend.foreach(onNext)
      q = toKeep

    case Cancel =>
      onComplete()
  }
}