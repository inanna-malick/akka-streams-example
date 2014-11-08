Scraping Reddit with Akka Streams 0.9
=====================================

Motivation
----------
Reddit offers convenient APIs for accessing content. In this post, I'm going to walk you through using akka-streams to grab the top X comments for each of the top Y posts in each of the top Z subreddits, persist wordcounts for each subreddit, and write the results to disk as .tsv files. Since we'd rather not have our IPs banned, we want to issue these API calls at consistent intervals, instead of short bursts. 

API Sketch:
-----------

These are the types and functions we'll be working with.

```scala
//types
type WordCount = Map[String, Int] 
type Subreddit = String  
case class LinkListing(links: Seq[Link])
case class Link(id: String, subreddit: Subreddit)
case class CommentListing(subreddit: Subreddit, comments: Seq[Comment])
case class Comment(subreddit: Subreddit, body: String)

//reddit API
def popularLinks(subreddit: Subreddit)(implicit ec: ExecutionContext): Future[LinkListing]
def popularComments(link: Link)(implicit ec: ExecutionContext): Future[CommentListing]
def popularSubreddits(implicit ec: ExecutionContext): Future[Seq[Subreddit]]

// mock KV store
def addWords(subreddit: Subreddit, words: WordCount): Future[Unit]
def wordCounts: Future[Map[Subreddit, WordCount]]
```

Naive Solution:
--------------

Since our API functions yield futures, the simplest possible solution is to use flatMap and Future.sequence to chain together functions.

```scala
def fetchPopularLinks(): Future[Seq[Link]] = 
  popularSubreddits
    .flatMap( subreddits => Future.sequence(subreddits.map(popularLinks)) )
    .map( linkListings => linkListings.flatMap(_.links) )

def fetchPopularComments(linksF: Future[Seq[Link]]): Future[Seq[Comment]] = 
  linksF
    .flatMap( links =>  Future.sequence(links.map(popularComments)))
    .map( commentListings => commentListings.flatMap(_.comments) )

def run(){
  val linksF = fetchPopularLinks()
  val commentsF = fetchPopularComments(linksF)
  commentsF.onSuccess{ case comments: Seq[Comment] => 
    println(s"fetched ${comments.length} comments")
  }
}
```

This example fetches a list of subreddit names, then issues a batch of requests for the top links in each subreddit. After fetching the links, it issues requests for the top comments on each link. This pattern of issuing bursts of requests works fine at first, then starts failing with 503: service unavailable errors as Reddit's rate limiting kicks in. Try it for yourself: open up a console and type 'main.Simple.run()' (you'll want to kill the process after calls start failing with 503 errors)


Streams 101
-----------

[Scala DSL](http://doc.akka.io/api/akka-stream-and-http-experimental/0.9/index.html#akka.stream.scaladsl.package): This domain specific language is used to create immutable stream descriptions that can be shared, composed, and materialized to create live streams composed of reactive stream primitives.
- Flows: stream descriptions with an open input. Can be attached to a subscriber and materialized, or consumed directly with foreach.
- Ducts: Flows, but with hanging input and output. Can be attached to a producer and subscriber, or appended to a flow
- Duct is a free-floating transformation pipeline to which a subscriber and producer can be attached. They can be appended to each other, or to flows, which describe producers of values

[Reactive Stream Primitives](https://github.com/reactive-streams/reactive-streams): Streams built from these primitives are live streams, that can process elements and exert backpressure. They are what is created when Scala DSL entities are materialized.
- A Subscriber is a component that accepts a sequenced stream of elements provided by a Publisher. It can 
- A Publisher is a provider of a potentially unbounded number of sequenced elements, publishing them according to the demand received from its Subscriber(s).


Less Naive Solution
-------------------

What we want is to issue requests at some regular interval. This will stop us from getting blocked for berserking their servers with thousands of requests in a short amount of time. We're going to do this using Akka's new stream library. First, we will build our transformation pipeline using Ducts. For now, assume we have some source of Subreddit names.
    

First we need a Duct[Subreddit, Comment] to turn the stream of subreddits produced by that source into a stream of comments from different subreddits. 

```scala
def fetchComments: Duct[Subreddit, Comment] = 
  // 0) Create a duct that applies no transformations.
  Duct[Subreddit] 
      // 1) Throttle the rate at which the next step can receive subreddit names.
      .zip(throttle.toPublisher).map{ case (t, Tick) => t } 
      // 2) Fetch links. Subject to rate limiting.
      .mapFuture( subreddit => RedditAPI.popularLinks(subreddit) ) 
      // 3) Flatten a stream of link listings into a stream of links.
      .mapConcat( listing => listing.links ) 
      // 4) Throttle the rate at which the next step can receive links.
      .zip(throttle.toPublisher).map{ case (t, Tick) => t } 
      // 5) Fetch links. Subject to rate limiting.
      .mapFuture( link => RedditAPI.popularComments(link) ) 
      // 6) Flatten a stream of comment listings into a stream of comments.
      .mapConcat( listing => listing.comments )
```


We're also going to need to calculate word counts and write them to some store, ideally saving up comments to avoid an IO operation per comment. 

```scala
val persistBatch: Duct[Comment, Int] = 
  // 0) Create a duct that applies no transformations.
  Duct[Comment]
      // 1) Group comments, emitting a batch every 5000 elements
      //    or every 5 seconds, whichever comes first.
      .groupedWithin(5000, 5 second) 
      // 2) Group comments by subreddit and write the wordcount 
      //    for each group to the store. This step outputs 
      //    the size of each batch after it is persisted.
      .mapFuture{ batch => 
        val grouped: Map[Subreddit, WordCount] = batch
          .groupBy(_.subreddit)
          .mapValues(_.map(_.toWordCount).reduce(merge))
        val fs = grouped.map{ case (subreddit, wordcount) => 
            store.addWords(subreddit, wordcount)
          }
        Future.sequence(fs).map{ _ => batch.size }
      }
```

So far, no processing has occurred. We've just described what we want to do. Now we create a starting flow of Subreddit names to which we append the Ducts created in the last two steps, yielding a single `Flow[Int]` the we can materialize and run.

```scala
  def main(args: Array[String]): Unit = {
    // 0) Create a Flow of Subreddit names, using either
    //    the argument vector or the result of an API call.
    val subreddits: Flow[Subreddit] =
      if (args.isEmpty) 
        Flow(RedditAPI.popularSubreddits).mapConcat(identity)
      else
        Flow(args.toVector)

    // 1) Append ducts to the initial flow and materialize it via forEach. 
    //    The resulting future succeeds if stream processing completes 
    //    or fails if an error occurs.
    val streamF: Future[Unit] = 
      subreddits
        .append(fetchComments)
        .append(persistBatch)
        .foreach{ n => println(s"persisted $n comments")}

    // 2) When stream processing is finished, load the resulting 
    //    wordcounts from the store, log some basic statisitics, 
    //    and write them to a .tsv files (code omited for brevity)
  }
```

Closing: 
--------

1. New Scala DSL. Largely similar, supports building complex [flow graphs](http://akka.io/news/2014/09/12/akka-streams-0.7-released.html). I'll rewrite this using the new DSL at some point, probably after akka-streams 1.0 comes out. It also offers new combinators such as mapAsyncUnordered, which is identical to mapFuture except for not preserving order. This can be used to allow stream processing to continue at speed despite the occasional long-running call.
2. Thanks to the Akka team and everyone at Typesafe for great open source software, Viktor Klang for coming to the US to give a talk and DataXu for hosting it, and of course thanks to my employer Aquto for letting me work with cool new technology.
