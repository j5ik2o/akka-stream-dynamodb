package jmh

import java.net.URI
import java.util.UUID
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Supplier

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import dynamodb.v2.{DynamoDBContainerHelper, DynamoDBStreamClient}
import org.openjdk.jmh.annotations._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BatchGetItemRequest,
  BatchWriteItemRequest,
  BatchWriteItemResponse,
  KeysAndAttributes,
  PutItemRequest,
  PutRequest,
  WriteRequest
}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SampleTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class DynamoDBBenchmark extends DynamoDBContainerHelper {
  var system: ActorSystem                = _
  var client: DynamoDbAsyncClient        = _
  var streamClient: DynamoDBStreamClient = _

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
    streamClient = new DynamoDBStreamClient(client)

    val requestItems =
      (for (i <- 1 to 100) yield createWriteRequest(s"batch-get-item-$i", i))
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
  def client_batchGetItem_single(): Unit = {
    val keys: List[Map[String, AttributeValue]] =
      (for (i <- 1 to 100)
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
  def client_batchGetItem_multi(): Unit = {
    val keys: List[Map[String, AttributeValue]] =
      (for (i <- 1 to 100)
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
  def stream_batchGetItem_single(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to 100)
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
    val future = streamClient.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def stream_batchGetItem_multi(): Unit = {
    implicit val s = system
    val keys =
      for (i <- 1 to 100)
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
    val future = streamClient.batchGetItemSource(request).runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def client_batchWriteItem_single(): Unit = {
    val requestItems =
      (for (i <- 1 to 150) yield createWriteRequest(s"client-batch-write-item-single-$i", i)).toList
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
  def stream_batchWriteItem_single(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to 150) yield createWriteRequest(s"stream-batch-write-item-single-$i", i))
    val input = Map("table1" -> requestItems.asJava).asJava
    val future = streamClient
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
  def client_batchWriteItem_multi(): Unit = {
    val requestItems =
      (for (i <- 1 to 150) yield createWriteRequest(s"client-batch-write-item-multi-$i", i)).toList
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
  def stream_batchWriteItem_multi(): Unit = {
    implicit val s = system
    val requestItems =
      (for (i <- 1 to 150) yield createWriteRequest(s"stream-batch-write-item-multi-$i", i))
    val input = Map("table1" -> requestItems.asJava, "table2" -> requestItems.asJava).asJava
    val future = streamClient
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
  def client_putItem(): Unit = {
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
  def stream_putItem(): Unit = {
    implicit val s = system
    val future = streamClient
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
