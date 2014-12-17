Scraping Reddit with Akka Streams 1.0
=====================================
<img src="img/mugatu_streams.jpg" alt="alt text" width="150">
Streams are so hot right now


- Streaming video is replacing legacy media.
    + netflix alone has been measured using 35% US downstream internet bandwidth at peak
- Real time data processing: streams of stock prices, analytics data, log events
- Big Data: processing large data sets can usually be reduced to some combination of map and reduce steps, both of which are naturally implemented using streams
- Internet of Things: streams of data produced by networked devices and sensors
- The Web: Streams of packets via TCP or UDP


- Reactive Streams is a new stream processing standard that uses a downstream channel to transfer data and an upstream channel to signal demand.
- Reactive Streams components switch between push and pull dynamically.
<img src="img/stream.png" alt="alt text" height="200">

- Upstream components only push data when the downstream signals demand. Demand is tracked as an integer and not a boolean, so the upstream is free to fulfill demand by sending individual elements or batches of a thousand.
- As a result of this, Reactive Streams switch between push and pull based on conditions.
- If demand exists the upstream pushes data downstream as soon as it becomes available.
- If the demand is exhausted, the upstream component only pushes data as a response to the downstream signalling demand.
- If the downstream is overloaded, lack of demand propagates upstream as buffers fill and components stop demanding new data.
- The source can then choose between slowing down (streaming a movie) and dropping data (processing real time data).

Streams can be merged, which splits the upstream demand

<img src="img/merge.png" alt="alt text" height="200">

Streams can be split, which merges the downstream demand.

<img src="img/split.png" alt="alt text" height="200">


- RS is defined by a minimal, heavily tested spec.

Here's the entire interface specification. (The behavior specification and tests are much longer)
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


Akka Streams DSL:
--------------------------

- However, those interfaces are too low level to code against.
- Akka streams also includes a high-level, type safe DSL for working with streams.
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

- Sources[Out]: produces elements of type Out:
- Sources can be created from the elements stored in a Vector.
```
val subreddits: Source[String] = Source(args.toVector)
```

- example from Future, with subsequent map concat step
- Sources can also be created from Futures, resulting in a Source that emits the result of the future or fails with an error if the future fails.
- Since popularSubreddits returns a Seq[String], we take the additional step of using the mapConcat utility function to flatten the Source[Seq[T]] into a Source[T]
  + mapConcat applies a function returning Seq[A] to a Source[B], returning a Source[A].

```
val subreddits: Source[String] = Source(RedditAPI.popularSubreddits).mapConcat(identity)
```


Sinks[In]: consumes elements of type In:
- some sinks produce values on completion, such as ForeachSink => Future[Unit] or FoldSink => Future[T]
- This sink takes a stream of comments, converts them into (subreddit, wordcount) pairs, and merges those pairs into a Map[String, WordCount] that is produced on stream completion

```
val wordCountSink: FoldSink[Map[String, WordCount], Comment] =
  FoldSink(Map.empty[String, WordCount])( 
    (acc: Map[String, WordCount], c: Comment) => 
      mergeWordCounts(acc, Map(c.subreddit -> c.toWordCount))
  )
```

Flows[In, Out]
- start by creating a Flow[String, String] that applies no transformations
- using via to concat a throttle flow which limits processing speed 
    + we'll show how throttle works later. For now, just think of it as a flow that applies no transformation but refuses to produce more than 1 element per rate
- using mapconcat again
- using mapAsyncUnordered

```
  val fetchLinks: Flow[String, Link] =
    Flow[String]
    .via(throttle(redditAPIRate))
    .mapAsyncUnordered( subreddit => RedditAPI.popularLinks(subreddit) )
    .mapConcat( listing => listing.links )
```

The flow that converts a stream of links into a stream of comments is similar.

```
val fetchComments: Flow[Link, Comment] =
  Flow[Link]
    .via(throttle(redditAPIRate))
    .mapAsyncUnordered( link => RedditAPI.popularComments(link) )
    .mapConcat( listing => listing.comments )
```



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


in conclusion
- damn, that was easy
- look at all those vals: wordCount, fetchLinks, fetchComments
- can be reused, thread safe, stored on objects instead of created for each stream
- great for more complex problems, by a team w/ history of implementing great stuff (Akka, Typesafe)
