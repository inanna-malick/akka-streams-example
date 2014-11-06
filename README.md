Scraping Reddit with Akka Streams 0.9
=====================================

Motivation
----------
Reddit offers convenient APIs for accessing content. In this post, I'm going to walk you through using akka-streams to grab the top X comments for each of the top Y posts in each of the top Z subreddits, persisting wordcounts for each subreddit, and writing the results to disk as TSV files. Since we'd like to be good internet citizens, (and not get our IP banned) we want to issue these api calls at consistient intervals, instead of short bursts. 

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

This fetches a list of subreddit names, then immediately issues requests for the top links for each subreddit. After fetching the links, it issues requests for the top comments for each link. This pattern of issuing bursts of requests works fine at first, then starts failing with 503: service unavailable errors as rate limiting kicks in. Try it for yourself: open up a console and type 'main.Simple.run()' (you'll want to kill the process after seeing the expected stream of 503 errors)


Streams
-------

What we want is to issue requests at configurable intervals. This will stop us from getting blocked for berzerking their servers with thousands of requests in a short amount of time. (However you feel about reddit as a community, DDOS'ing them is still rude)

We're going to do this using akka's new stream library. Let's start by describing the transformations our stream will need to perform using Ducts and Duct combinator functions.
Explain concept of ducts in brief, link to scaladoc.

We're going to need a Duct[Subreddit, Comment] to turn our starting stream of subreddits into a stream of comments. It will issue API calls to get links for each subreddit, comments for each link, etc.

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


We're also going to use a Duct[Comment, Int] to persist batches of comments, outputing the size of the persisted batches. 
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

note that the above are vals, not defs. They can be reused, and no processing occurs until they are materialized. Cool, no?

final append: no processing has occured, no api calls made. We've just described what we want to do. Now make it so.
Having created these high-level descriptions of computations to be performed, we can then append them to a Flow\[Subreddit\] \(created using the result of the popular Subreddits api call or the list of subreddits provided as command line arguments if present\)


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
    
    1. subreddits
        + args as vector -> flow
        + future result -> flow (+ mapConcat, but that's explained above)

    2. append(s)

    3. foreach (consumes stream, runs a function on each resulting element

Closing: 
--------
things that could be better, mention new flow graph for complex topologies. (this is scalaDSL, see scalaDSL2 for the newness). Also props to akka dudes, vKlang for talk, dataxu for hosting
