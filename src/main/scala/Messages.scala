package com.pkinsky

import play.api.libs.json.{Reads, Writes, Json}
import scala.collection.immutable

case class WordCountRequest(subreddits: immutable.Iterable[String])

object WordCountRequest{
  implicit val format = Json.format[WordCountRequest]
}

case class WordCountResult(subreddit: String, wordcounts: Map[String, Int])

object WordCountResult{
  implicit val format = Json.format[WordCountResult]
}


case class WordCountResponse(error: Option[String], result: Option[WordCountResult])

object WordCountResponse{
  implicit val format = Json.format[WordCountResponse]
}
