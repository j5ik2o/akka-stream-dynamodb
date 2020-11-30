package jmh

import java.net.URI
import java.util.UUID
import java.util.concurrent.{CompletableFuture, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import dynamodb.v2.{DynamoDBContainerHelper, DynamoDBStreamClient, FlowMode, FlowModeWithPublisher}
import org.openjdk.jmh.annotations._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class DynamoDBBenchmark extends DynamoDBContainerHelper {
  var system: ActorSystem                                    = _
  var client: DynamoDbAsyncClient                            = _
  var streamClient: DynamoDBStreamClient                     = _
  var streamClientForJava: DynamoDBStreamClient              = _
  var streamClientForScalaJDK: DynamoDBStreamClient          = _
  var streamClientForScalaCompat: DynamoDBStreamClient       = _
  var streamClientForJavaWithPublisher: DynamoDBStreamClient = _

  val batchGetItemTotalSize   = DynamoDBStreamClient.BatchGetItemMaxSize * 5
  val batchWriteItemTotalSize = DynamoDBStreamClient.BatchWriteItemMaxSize * 5

  @Setup
  def setup(): Unit = {
    dynamoDbLocalContainer.start()
    createTable(Seq("table1", "table2"))
    system = ActorSystem("benchmark-" + UUID.randomUUID().toString)
    implicit val s = system
    client = DynamoDbAsyncClient
      .builder()
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
      )
      .endpointOverride(URI.create(dynamoDBEndpoint))
      .build()
    streamClient = new DynamoDBStreamClient(client = client)

    streamClientForJava = new DynamoDBStreamClient(
      client = client,
      putItemFlowMode = FlowMode.Java,
      getItemFlowMode = FlowMode.Java,
      deleteItemFlowMode = FlowMode.Java,
      batchGetItemFlowMode = FlowModeWithPublisher.Java,
      batchWriteItemFlowMode = FlowMode.Java,
      queryFlowMode = FlowModeWithPublisher.Java,
      scanFlowMode = FlowModeWithPublisher.Java
    )
    streamClientForScalaJDK = new DynamoDBStreamClient(
      client = client,
      putItemFlowMode = FlowMode.ScalaJDK,
      getItemFlowMode = FlowMode.ScalaJDK,
      deleteItemFlowMode = FlowMode.ScalaJDK,
      batchGetItemFlowMode = FlowModeWithPublisher.ScalaJDK,
      batchWriteItemFlowMode = FlowMode.ScalaJDK,
      queryFlowMode = FlowModeWithPublisher.ScalaJDK,
      scanFlowMode = FlowModeWithPublisher.ScalaJDK
    )
    streamClientForScalaCompat = new DynamoDBStreamClient(
      client = client,
      putItemFlowMode = FlowMode.ScalaCompat,
      getItemFlowMode = FlowMode.ScalaCompat,
      deleteItemFlowMode = FlowMode.ScalaCompat,
      batchGetItemFlowMode = FlowModeWithPublisher.ScalaCompat,
      batchWriteItemFlowMode = FlowMode.ScalaCompat,
      queryFlowMode = FlowModeWithPublisher.ScalaCompat,
      scanFlowMode = FlowModeWithPublisher.ScalaCompat
    )
    streamClientForJavaWithPublisher = new DynamoDBStreamClient(
      client = client,
      putItemFlowMode = FlowMode.Java,
      getItemFlowMode = FlowMode.Java,
      deleteItemFlowMode = FlowMode.Java,
      batchGetItemFlowMode = FlowModeWithPublisher.Publisher,
      batchWriteItemFlowMode = FlowMode.Java,
      queryFlowMode = FlowModeWithPublisher.Publisher,
      scanFlowMode = FlowModeWithPublisher.Publisher
    )

    val requestItems =
      (for (i <- 1 to batchGetItemTotalSize) yield createWriteRequest(s"batch-get-item-$i", i))
    val batchWriteItemFuture = streamClient
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(
            Map("table1" -> requestItems.asJava, "table2" -> requestItems.asJava).asJava
          )
          .build()
      )
      .runWith(Sink.seq)
    Await.result(batchWriteItemFuture, Duration.Inf)
  }

  @TearDown
  def tearDown(): Unit = {
    dynamoDbLocalContainer.stop()
    system.terminate()
  }

  def createWriteRequest(pid: String, seqNr: Long): WriteRequest = {
    WriteRequest
      .builder()
      .putRequest(
        PutRequest
          .builder()
          .item(
            Map(
              "pkey"  -> AttributeValue.builder().s(pid).build(),
              "skey"  -> AttributeValue.builder().s(seqNr.toString).build(),
              "value" -> AttributeValue.builder().s("test").build()
            ).asJava
          )
          .build()
      )
      .build()
  }

  @Benchmark
  def batchGetItem_single_javaClient(): Unit = {
    val keys: List[Map[String, AttributeValue]] =
      (for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )).toList
    val input = Map("table1" -> keys)
    def loop(
        map: Map[String, List[Map[String, AttributeValue]]],
        future: CompletableFuture[Unit]
    ): CompletableFuture[Unit] = {
      map.filterNot { case (_, v) => v.isEmpty } match {
        case m if m.isEmpty => future
        case l =>
          val nm = l.map {
            case (key, values) =>
              val (h, t) =
                values.splitAt(DynamoDBStreamClient.BatchGetItemMaxSize / l.keys.size)
              key -> (h, t)
          }
          val ri = nm.map {
            case (k, (v, _)) =>
              val nv = KeysAndAttributes
                .builder()
                .keys(v.map(_.asJava).asJava)
                .build()
              (k, nv)
          }.asJava
          val t = nm.map { case (k, (_, v)) => (k, v) }
          val request =
            BatchGetItemRequest
              .builder()
              .requestItems(ri)
              .build()
          val f = client
            .batchGetItem(request)
            .thenApply(_ => ())
          future.thenCompose(_ => f)
          loop(t, f)
      }
    }
    loop(input, CompletableFuture.completedFuture(())).join()
  }

  @Benchmark
  def batchGetItem_multi_javaClient(): Unit = {
    val keys: List[Map[String, AttributeValue]] =
      (for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )).toList
    val input = Map("table1" -> keys, "table2" -> keys)
    def loop(
        map: Map[String, List[Map[String, AttributeValue]]],
        future: CompletableFuture[Unit]
    ): CompletableFuture[Unit] = {
      map.filterNot { case (_, v) => v.isEmpty } match {
        case m if m.isEmpty => future
        case l =>
          val nm = l.map {
            case (key, values) =>
              val (h, t) =
                values.splitAt(DynamoDBStreamClient.BatchGetItemMaxSize / l.keys.size)
              key -> (h, t)
          }
          val ri = nm.map {
            case (k, (v, _)) =>
              val nv = KeysAndAttributes
                .builder()
                .keys(v.map(_.asJava).asJava)
                .build()
              (k, nv)
          }.asJava
          val t = nm.map { case (k, (_, v)) => (k, v) }
          val request =
            BatchGetItemRequest
              .builder()
              .requestItems(ri)
              .build()
          val f: CompletableFuture[Unit] = client
            .batchGetItem(request)
            .thenApply(_ => ())
          future.thenCompose(_ => f)
          loop(t, f)
      }
    }
    loop(input, CompletableFuture.completedFuture(())).join()
  }

  @Benchmark
  def batchGetItem_single_akka_javaFlow(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )
    val keysAndAttributes = KeysAndAttributes
      .builder()
      .keys(keys.map(_.asJava).asJava)
      .build()
    val request =
      BatchGetItemRequest
        .builder()
        .requestItems(Map("table1" -> keysAndAttributes).asJava)
        .build()
    val future = streamClientForJava.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchGetItem_single_akka_javaFlow_publisher(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )
    val keysAndAttributes = KeysAndAttributes
      .builder()
      .keys(keys.map(_.asJava).asJava)
      .build()
    val request =
      BatchGetItemRequest
        .builder()
        .requestItems(Map("table1" -> keysAndAttributes).asJava)
        .build()
    val future = streamClientForJavaWithPublisher.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchGetItem_single_akka_compatFlow(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )
    val keysAndAttributes = KeysAndAttributes
      .builder()
      .keys(keys.map(_.asJava).asJava)
      .build()
    val request =
      BatchGetItemRequest
        .builder()
        .requestItems(Map("table1" -> keysAndAttributes).asJava)
        .build()
    val future = streamClientForScalaCompat.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchGetItem_single_akka_jdkFlow(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )
    val keysAndAttributes = KeysAndAttributes
      .builder()
      .keys(keys.map(_.asJava).asJava)
      .build()
    val request =
      BatchGetItemRequest
        .builder()
        .requestItems(Map("table1" -> keysAndAttributes).asJava)
        .build()
    val future = streamClientForScalaJDK.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchGetItem_multi_akka_javaFlow(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )
    val keysAndAttributes = KeysAndAttributes
      .builder()
      .keys(keys.map(_.asJava).asJava)
      .build()
    val input = Map("table1" -> keysAndAttributes, "table2" -> keysAndAttributes).asJava
    val request =
      BatchGetItemRequest
        .builder()
        .requestItems(input)
        .build()
    val future = streamClientForJava.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchGetItem_multi_akka_javaFlow_publisher(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )
    val keysAndAttributes = KeysAndAttributes
      .builder()
      .keys(keys.map(_.asJava).asJava)
      .build()
    val input = Map("table1" -> keysAndAttributes, "table2" -> keysAndAttributes).asJava
    val request =
      BatchGetItemRequest
        .builder()
        .requestItems(input)
        .build()
    val future = streamClientForJavaWithPublisher.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchGetItem_multi_akka_compatFlow(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )
    val keysAndAttributes = KeysAndAttributes
      .builder()
      .keys(keys.map(_.asJava).asJava)
      .build()
    val input = Map("table1" -> keysAndAttributes, "table2" -> keysAndAttributes).asJava
    val request =
      BatchGetItemRequest
        .builder()
        .requestItems(input)
        .build()
    val future = streamClientForScalaCompat.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchGetItem_multi_akka_jdkFlow(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to batchGetItemTotalSize)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )
    val keysAndAttributes = KeysAndAttributes
      .builder()
      .keys(keys.map(_.asJava).asJava)
      .build()
    val input = Map("table1" -> keysAndAttributes, "table2" -> keysAndAttributes).asJava
    val request =
      BatchGetItemRequest
        .builder()
        .requestItems(input)
        .build()
    val future = streamClientForScalaJDK.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchWriteItem_single_javaClient(): Unit = {
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"client-batch-write-item-single-$i", i)).toList
    val input = Map("table1" -> requestItems)
    def loop(
        map: Map[String, List[WriteRequest]],
        future: CompletableFuture[Unit]
    ): CompletableFuture[Unit] = {
      map.filterNot { case (_, v) => v.isEmpty } match {
        case m if m.isEmpty => future
        case l =>
          val nm = l.map {
            case (key, values) =>
              val (h, t) =
                values.splitAt(DynamoDBStreamClient.BatchWriteItemMaxSize / l.keys.size)
              (key, (h, t))
          }
          val ri = nm.map { case (k, v) => (k, v._1.asJava) }.asJava
          val t  = nm.map { case (k, v) => (k, v._2) }
          val f: CompletableFuture[Unit] = client
            .batchWriteItem(
              BatchWriteItemRequest
                .builder()
                .requestItems(ri)
                .build()
            )
            .thenApply(_ => ())
          future.thenCompose(_ => f)
          loop(t, f)
      }
    }
    loop(input, CompletableFuture.completedFuture(())).join()
  }

  @Benchmark
  def batchWriteItem_single_akka_javaFlow(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"stream-batch-write-item-single-$i", i))
    val input = Map("table1" -> requestItems.asJava).asJava
    val future = streamClientForJava
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(input)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchWriteItem_single_akka_javaFlow_publisher(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"stream-batch-write-item-single-$i", i))
    val input = Map("table1" -> requestItems.asJava).asJava
    val future = streamClientForJavaWithPublisher
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(input)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchWriteItem_single_akka_compatFlow(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"stream-batch-write-item-single-$i", i))
    val input = Map("table1" -> requestItems.asJava).asJava
    val future = streamClientForScalaCompat
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(input)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchWriteItem_single_akka_jdkFlow(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"stream-batch-write-item-single-$i", i))
    val input = Map("table1" -> requestItems.asJava).asJava
    val future = streamClientForScalaJDK
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(input)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchWriteItem_multi_javaClient(): Unit = {
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"client-batch-write-item-multi-$i", i)).toList
    val input = Map("table1" -> requestItems, "table2" -> requestItems)
    def loop(
        map: Map[String, List[WriteRequest]],
        future: CompletableFuture[Unit]
    ): CompletableFuture[Unit] = {
      map.filterNot { case (_, v) => v.isEmpty } match {
        case m if m.isEmpty => future
        case l =>
          val nm = l.map {
            case (key, values) =>
              val (h, t) =
                values.splitAt(DynamoDBStreamClient.BatchWriteItemMaxSize / l.keys.size)
              (key, (h, t))
          }
          val ri = nm.map { case (k, v) => (k, v._1.asJava) }.asJava
          val t  = nm.map { case (k, v) => (k, v._2) }
          val f: CompletableFuture[Unit] = client
            .batchWriteItem(
              BatchWriteItemRequest
                .builder()
                .requestItems(ri)
                .build()
            )
            .thenApply(_ => ())
          future.thenCompose(_ => f)
          loop(t, f)
      }
    }
    loop(input, CompletableFuture.completedFuture(())).join()
  }

  @Benchmark
  def batchWriteItem_multi_akka_javaFlow(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"stream-batch-write-item-multi-$i", i))
    val input = Map("table1" -> requestItems.asJava, "table2" -> requestItems.asJava).asJava
    val future = streamClientForJava
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(input)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchWriteItem_multi_akka_javaFlow_publisher(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"stream-batch-write-item-multi-$i", i))
    val input = Map("table1" -> requestItems.asJava, "table2" -> requestItems.asJava).asJava
    val future = streamClientForJavaWithPublisher
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(input)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchWriteItem_multi_akka_compatFlow(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"stream-batch-write-item-multi-$i", i))
    val input = Map("table1" -> requestItems.asJava, "table2" -> requestItems.asJava).asJava
    val future = streamClientForScalaCompat
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(input)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def batchWriteItem_multi_akka_jdkFlow(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to batchWriteItemTotalSize)
        yield createWriteRequest(s"stream-batch-write-item-multi-$i", i))
    val input = Map("table1" -> requestItems.asJava, "table2" -> requestItems.asJava).asJava
    val future = streamClientForScalaJDK
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(input)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def putItem_javaClient(): Unit = {
    client
      .putItem(
        PutItemRequest
          .builder()
          .tableName("table1")
          .item(
            Map(
              "pkey"  -> AttributeValue.builder().s("client-put-item-1").build(),
              "skey"  -> AttributeValue.builder().s("0").build(),
              "value" -> AttributeValue.builder().s("test").build()
            ).asJava
          )
          .build()
      )
      .join()
  }

  @Benchmark
  def putItem_akka_javaFlow(): Unit = {
    implicit val s = system
    val future = streamClientForJava
      .putItemSource(
        PutItemRequest
          .builder()
          .tableName("table1")
          .item(
            Map(
              "pkey"  -> AttributeValue.builder().s("stream-put-item-1").build(),
              "skey"  -> AttributeValue.builder().s("0").build(),
              "value" -> AttributeValue.builder().s("test").build()
            ).asJava
          )
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def putItem_akka_compatFlow(): Unit = {
    implicit val s = system
    val future = streamClientForScalaCompat
      .putItemSource(
        PutItemRequest
          .builder()
          .tableName("table1")
          .item(
            Map(
              "pkey"  -> AttributeValue.builder().s("stream-put-item-1").build(),
              "skey"  -> AttributeValue.builder().s("0").build(),
              "value" -> AttributeValue.builder().s("test").build()
            ).asJava
          )
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def putItem_akka_jdkFlow(): Unit = {
    implicit val s = system
    val future = streamClientForScalaJDK
      .putItemSource(
        PutItemRequest
          .builder()
          .tableName("table1")
          .item(
            Map(
              "pkey"  -> AttributeValue.builder().s("stream-put-item-1").build(),
              "skey"  -> AttributeValue.builder().s("0").build(),
              "value" -> AttributeValue.builder().s("test").build()
            ).asJava
          )
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

}
