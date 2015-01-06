Scraping Reddit with Akka Streams 1.0
=====================================

<img src="img/mugatu_streams.png" alt="alt text">


**Assertion**: A large fraction of today's tech industry can be described as some combination of sending, transforming and consuming streams of data.
- Streaming audio and video are quickly replacing legacy media delivery systems like Blockbuster: Netflix alone has been measured using 35% percent of the US's downstream internet bandwidth during peak hours. 
Meanwhile, a significant fraction of the other 65% of bandwidth is taken up by streams of stock prices, analytics data, log events, streams of data from networked sensors.
  + huge assertion, could do with some sources re significant fraction
- Big data, the buzzword of our times, largely boils down to sequences of map & reduce steps, which can easily be expressed in terms of stream processing pipelines. Writing jobs as stream processing pipelines adds flexibility. For example, Twitter uses a library called Summingbird to transform high-level stream processing steps into either real-time data processing topologies using Storm or batch-processing jobs using Hadoop.
- And speaking of the Internet:
  + TCP itself is just a way of sending ordered streams of packets between hosts
  + UDP is another way to send streams of data, without TCP's ordering or delivery guarantees


Reactive Streams
----------------
<img src="img/stream.png" alt="alt text" height="200">

The Reactive Streams standard defines an upstream demand channel and a downstream data channel. Publishers do not send data until a request for N elements arrives via the demand channel, at which point they are free to push up to N elements downstream either in batches or individually. When outstanding demand exists, the publisher is free to push data to the subscriber as it becomes available. When demand is exhausted, the publisher cannot send data except as a response to demand signalled from downstream. This lack of demand, or backpressure, propagates upstream in a controlled manner, allowing the source node to choose between starting up more resources, slowing down, or dropping data.


What's cool is that since data and demand travel in opposite directions, merging streams splits the upstream demand and splitting streams merges the downstream demand.

<img src="img/merge.png" alt="alt text" height="200">
<img src="img/split.png" alt="alt text" height="200">

The Code
--------
RS is defined by the following minimal, heavily tested, set of interfaces.
```
trait Publisher[T] {
  def subscribe(s: Subscriber[T]): Unit
}

trait Subscriber[T] {
    def onSubscribe(s: Subscription): Unit
    def onNext(t: T): Unit
    def onError(t: Throwable): Unit
    def onComplete(): Unit
}

trait Subscription {
   def request(n: Unit): Unit
   def cancel(): Unit
}
```

This is great, but it's all very low level. Look at all those Unit return types! If only there was a domain-specific language for transforming and composing stream processing graphs.

Akka Streams DSL:
--------------------------

- Akka streams also includes a high-level, type safe DSL for working with streams.
- This DSL is used to create descriptions of stream processing graphs, which are then materialized into running reactive streams.
- Domain Specific Language makes this easy, fun, typesafe to work with. Let me demonstrate w/ reddit example.
- We'll be working with this API:

```
type WordCount = Map[String, Int]
case class LinkListing(links: Seq[Link])
case class Link(id: String, subreddit: String)
case class CommentListing(subreddit: String, comments: Seq[Comment])
case class Comment(subreddit: String, body: String)

trait RedditAPI {
  def popularLinks(subreddit: String)(implicit ec: ExecutionContext): Future[LinkListing]
  def popularComments(link: Link)(implicit ec: ExecutionContext): Future[CommentListing]
  def popularStrings(implicit ec: ExecutionContext): Future[Seq[String]]
}
```

Sources
-------

An instance of the type Source[Out] produces a potentially unbounded stream of elements of type Out. We'll start by creating a stream of subreddit names, represented as Strings.

Sources can be created from the elements stored in a Vector.
```
val subreddits: Source[String] = Source(args.toVector)
```

Single-element sources can also be created using the results of a Future, resulting in a Source that either emits the result of the future if it is successful or fails with an error if the future fails.

```
val subreddits: Source[String] = Source(RedditAPI.popularSubreddits).mapConcat(identity)
```

Since popularSubreddits creates a `Future[Seq[String]]`, we take the additional step of using mapConcat to flatten the resulting Source[Seq[String]] into a Source[String]. The mapConcat method 'Transforms each input element into a sequence of output elements that is then flattened into the output stream'. Since we already have a Source[Seq[T]], we just use the identity function.

Sinks
-----

A Sink[In] consumes elements of type In.Some sinks produce values on completion. For example, ForeachSinks produce a Future[Unit] that completes when the stream completes. FoldSinks, which fold some number of elements A into a zero value B using a function (A, B) => B produce a Future[B] that completes when the stream completes.

This sink takes a stream of comments, converts them into (subreddit, wordcount) pairs, and merges those pairs into a Map[String, WordCount] that can be retrieved on stream completion

```
val wordCountSink: FoldSink[Map[String, WordCount], Comment] =
  FoldSink(Map.empty[String, WordCount])(
    (acc: Map[String, WordCount], c: Comment) =>
      mergeWordCounts(acc, Map(c.subreddit -> c.toWordCount))
  )
```

Flows
-----

A Flow[In, Out] consumes elements of type In, applies some sequence of transformations, and emits elements of type Out.

This Flow takes subreddit names and emits popular links for each supplied subreddit name.
- We start by creating a Flow[String, String], a pipeline that applies no transformations.
- We use via to append a throttle Flow.
    + We'll define throttle in the next section. For now, just think of it as a black box Flow[T, T] that limits throughput to one message per redditAPIRate time units.
- Next we use mapAsyncUnordered to fetch popular links for each subreddit name emitted by the throttle.
    + mapAsyncUnordered is used here because we don't care about preserving ordering. It emits elements as soon as their Future completes, which keeps the occasional long-running call from blocking the entire stream.
- Finally, we use mapConcat to flatten the resulting stream of LinkListings into a stream of Links.

```
  val fetchLinks: Flow[String, Link] =
    Flow[String]
    .via(throttle(redditAPIRate))
    .mapAsyncUnordered( subreddit => RedditAPI.popularLinks(subreddit) )
    .mapConcat( listing => listing.links )
```

This flow uses the same sequence of steps (with a different API call) to convert a stream of links into a stream of the most popular comments on those links.
```
val fetchComments: Flow[Link, Comment] =
  Flow[Link]
    .via(throttle(redditAPIRate))
    .mapAsyncUnordered( link => RedditAPI.popularComments(link) )
    .mapConcat( listing => listing.comments )
```

Graphs
------

Sometimes a linear pipeline isn't enough, in those cases stream graphs can be constructed where nodes have multiple inputs or outputs.
    - using graphbuilder for connecting stream processing vertices. Graphs can be complete or partial, with partial graphs having undefined sources or sinks.
    - Partial graphs can be converted into sinks, sources or flows.
 - throttle step as used above. image first

```
def throttle[T](rate: FiniteDuration): Flow[T, T] = {
  val tickSource = TickSource(rate, rate, () => () )
  val zip = Zip[T, Unit]
  val in = UndefinedSource[T]
  val out = UndefinedSink[(T, Unit)]
  PartialFlowGraph{ implicit builder =>
    import FlowGraphImplicits._
    in ~> zip.left
    tickSource ~> zip.right
    zip.out ~> out
  }.toFlow(in, out).map{ case (t, _) => t }
}
```


Finally, we combine these steps to create a description of a stream processing graph, which we materialize and run with .runWith()

```
val res: Future[Map[String, WordCount]] =
  subreddits
    .via(fetchLinks)
    .via(fetchComments)
    .runWith(wordCountSink)

res.onComplete{
  case Success(wordcounts) =>
    writeResults(wordcounts)
    as.shutdown()
  case Failure(f) =>
    println(s"failed with $f")
    as.shutdown()
  }
```


Conclusion
----------

- We started by defining small pipeline segments like throttle, used them to build larger pipeline segments such as fetchLinks and fetchComments, then used these larger segments to create our stream processing graph. These smaller stream processing segments are immutable, thread safe, and fully reusable. They could easily be stored statically, on some object, to avoid the overhead of repeated initialization.
- Akka Streams is a great tool that can (and should) be used for more complex problems.
