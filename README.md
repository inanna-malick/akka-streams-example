Scraping Reddit with Akka Streams 1.0
=====================================

Streams are so hot right now 

- Streaming video is replacing legacy media.
      + netflix alone has been measured using 35% US downstream internet bandwidth at peak
- Real time data processing: streams of stock prices, analytics data, log events
- Big data: processing large data sets can usually be reduced to some combination of map and reduce steps, both of which are naturally implemented using streams
- The Internet: TCP and UDP are both standards for sending streams of packets with different deliverability guarantees

Reactive Streams is a new stream processing standard that explicitly models backpressure by using both a downstream data channel and an upstream demand channel. Upstream components push data based on downstream demand: when demand exists they push data downstream as it becomes available. If the demand is exhausted, the upstream component will only push data as a response to the downstream informing it that there is more demand. This backpressure can propagate upstream as buffers fill and components stop signalling demanding new data. The source can then choose between slowing down (streaming a movie) and dropping data (processing real time data). In other words, reactive streams components switch between pull and push dynamics depending on demand.

Streams can be merged and split, which has the opposite effect on backpressure: 
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
actor example

```
...etc
```

Akka Streams DSL:
--------------------------

Akka streams also includes a high-level, type safe DSL for working with streams. Some  examples,

```
val source: Source[A]
def isInteresting(a: A): Boolean
def extractFeatures(a: A): Seq[B]
pipeline: Flow[B, C]
sink: Sink[C]
```

The ease of combining them together:

```
source.filter(isInteresting).mapConcat(extractFeatures).via(pipeline).to(sink)
```

Sources, Flows and Sinks make it easy to construct and string together linear processing steps, graph builders are used to construct more complicated processing graphs. These graphs can either be complete graphs runnable by themselves or partial graphs with undefined sources, sinks or both that can be converted to Sources, Sinks or Flows. `~>` is used to connect vertices in a type safe manner:

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

maybe show image


mention demand from akka community?
- people wanted type-safe streaming through actors with bounded buffering
- flow control is non-trivial, mailboxes blow up if demand spikes

Example:
------------
-
to make this example concrete, let's build something: give types/reddit api spec,

````
type WordCount = Map[String, Int]
case class LinkListing(links: Seq[Link])
case class Link(id: String, subreddit: String)
case class CommentListing(subreddit: String, comments: Seq[Comment])
case class Comment(subreddit: String, body: String)

trait RedditAPI { // handles interaction with reddit's API,
  def popularLinks(subreddit: String)(implicit ec: ExecutionContext): Future[LinkListing]
  def popularComments(link: Link)(implicit ec: ExecutionContext): Future[CommentListing]
  def popularStrings(implicit ec: ExecutionContext): Future[Seq[String]]
}
```
We'll use the throttle defined earlier to restrict the flows that make API calls to 1 request per 500ms to avoid rate limiting.


transform a stream of subreddit names into a stream of the most popular links in posted to subreddits
  ```
  val fetchLinks: Flow[String, Link] =
    Flow[String]
        .via(throttle(redditAPIRate))
        .mapAsyncUnordered( subreddit => RedditAPI.popularLinks(subreddit) )
        .mapConcat( listing => listing.links )
```

transform a stream of links into a stream of the most popular comments on those links
```
comments in those threads
  val fetchComments: Flow[Link, Comment] =
    Flow[Link]
        .via(throttle(redditAPIRate))
        .mapAsyncUnordered( link => RedditAPI.popularComments(link) )
        .mapConcat( listing => listing.comments )
```


convert incoming comments into word counts and save them in an in-memory store
```
sink
  def wordCountSink: Sink[Comment] =
    Flow[Comment]
        .map{ c => (c.subreddit, c.toWordCount)}
        .to(Sink(WordCountSubscriber()))
```

subreddits: Source[String] stream of subreddit names from 
from input Vector
from API call then concat

```
val subreddits: Source[String] =
  Source(RedditAPI.popularSubreddits)
    .mapConcat(identity)
```


```    
    subreddits
      .via(fetchLinks)
      .via(fetchComments)
      .to(wordCountSink)
      .run
```

a few words on how it easy it was to define the stream processing components and connect them. Also all typesafe.

Conclusion
---------------
todo.
