Scraping Reddit with Akka Streams 1.0
=====================================

![Alt text](img/mugatu_streams.jpg "Optional Title")
Streams are so hot right now


- Streaming video is replacing legacy media.
    + netflix alone has been measured using 35% US downstream internet bandwidth at peak
- Real time data processing: streams of stock prices, analytics data, log events
- Big Data: processing large data sets can usually be reduced to some combination of map and reduce steps, both of which are naturally implemented using streams
- Internet of Things: streams of data produced by networked devices and sensors
- The Web: Streams of packets via TCP or UDP


Reactive Streams is a new stream processing standard with both a downstream data channel and an upstream demand channel. 

![Alt text](img/stream.png "Optional Title")

Upstream components push data based on downstream demand: when demand exists they push data downstream as it becomes available. If the demand is exhausted, the upstream component will only push data as a response to the downstream informing it that there is more demand. This backpressure can propagate upstream as buffers fill and components stop signalling demanding new data. The source can then choose between slowing down (streaming a movie) and dropping data (processing real time data). In other words, reactive streams components switch between pull and push dynamics depending on demand.

Streams can be merged
![Alt text](img/merge.png "Optional Title")
and split
![Alt text](img/split.png "Optional Title")

, which has the opposite effect on backpressure:
show pictures
introduces the idea that streams can be graphs
RS is based on a few heavily tested simple interfaces: (double check)

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

rigorous specification of semantics, heavily tested, simple enough complete freedom for APIs, different impls can interop seamlessly via these interfaces

Akka Streams DSL:
--------------------------

- However, those interfaces are too low level to code against.
- Akka streams also includes a high-level, type safe DSL for working with streams.
- dsl makes this easy, fun, typesafe to work with. Let me demonstrate w/ reddit example.
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

Sources[Out]: produces elements of type Out:
- example from Vector
```
val subreddits: Source[String] = Source(args.toVector)
```

example from future, with subsequent map concat step
describe mapconcat
```
val subreddits: Source[String] = Source(RedditAPI.popularSubreddits).mapConcat(identity)
```


Sinks[In]: consumes elements of type In:
some sinks produce values on completion, such as ForeachSink => Future[Unit] or FoldSink => Future[T]

```
val wordCountSink: FoldSink[Map[String, WordCount], Comment] =
  FoldSink(Map.empty[String, WordCount])( 
    (acc: Map[String, WordCount], c: Comment) => 
      mergeWordCounts(acc, Map(c.subreddit -> c.toWordCount))
  )
```

Flows[In, Out]
- subreddit name -> popular links
- using mapconcat again
- using mapAsyncUnordered
- using via to concat a throttle flow which limits processing speed

```
  val fetchLinks: Flow[String, Link] =
    Flow[String]
    .via(throttle(redditAPIRate))
    .mapAsyncUnordered( subreddit => RedditAPI.popularLinks(subreddit) )
    .mapConcat( listing => listing.links )
```

the flow that turns links into comments follows the same pattern

```
val fetchComments: Flow[Link, Comment] =
  Flow[Link]
    .via(throttle(redditAPIRate))
    .mapAsyncUnordered( link => RedditAPI.popularComments(link) )
    .mapConcat( listing => listing.comments )
```



Sometimes a linear stream isn't sufficient, in those cases stream graphs can be constructed.
 - using graphbuilder for connecting stream processing vertices. Graphs can be complete or partial, with partial graphs having undefined sources or sinks. Partial graphs can be converted into sinks, sources or flows.
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
