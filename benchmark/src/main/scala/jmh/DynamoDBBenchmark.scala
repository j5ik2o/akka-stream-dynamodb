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
          .requestItems(Map("table1" -> requestItems.asJava).asJava)
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
  def client_batchGetItem_signle(): Unit = {
    val keys: List[Map[String, AttributeValue]] =
      (for (i <- 1 to 100)
        yield Map(
          "pkey" -> AttributeValue.builder().s(s"batch-get-item-$i").build(),
          "skey" -> AttributeValue.builder().s(i.toString).build()
        )).toList
    def loop(
        requestItems: List[Map[String, AttributeValue]],
        future: CompletableFuture[Unit]
    ): CompletableFuture[Unit] = {
      requestItems match {
        case Nil => future
        case l =>
          val (h, t) = l.splitAt(10)
          val keysAndAttributes = KeysAndAttributes
            .builder()
            .keys(h.map(_.asJava).asJava)
            .build()
          val request =
            BatchGetItemRequest
              .builder()
              .requestItems(Map("table1" -> keysAndAttributes).asJava)
              .build()
          val f = client
            .batchGetItem(request)
            .thenApply(_ => ())
          future.thenCompose(_ => f)
          loop(t, f)
      }
    }
    loop(keys, CompletableFuture.completedFuture(())).join()
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
  def client_batchWriteItem_signle(): Unit = {
    val requestItems = (for (i <- 1 to 150) yield createWriteRequest(s"pid-1-$i", i)).toList
    def loop(
        requestItems: List[WriteRequest],
        future: CompletableFuture[Unit]
    ): CompletableFuture[Unit] = {
      requestItems match {
        case Nil => future
        case l =>
          val (h, t) = l.splitAt(15)
          val f = client
            .batchWriteItem(
              BatchWriteItemRequest
                .builder()
                .requestItems(Map("table1" -> h.asJava).asJava)
                .build()
            )
            .thenApply(_ => ())
          future.thenCompose(_ => f)
          loop(t, f)
      }
    }
    loop(requestItems, CompletableFuture.completedFuture(())).join()
  }

  @Benchmark
  def client_batchWriteItem_multi(): Unit = {
    val requestItems = (for (i <- 1 to 150) yield createWriteRequest(s"pid-1-$i", i)).toList
    def loop(
        requestItems: List[WriteRequest],
        future: CompletableFuture[Unit]
    ): CompletableFuture[Unit] = {
      requestItems match {
        case Nil => future
        case l =>
          val (h, t) = l.splitAt(6)
          val f = client
            .batchWriteItem(
              BatchWriteItemRequest
                .builder()
                .requestItems(Map("table1" -> h.asJava, "table2" -> h.asJava).asJava)
                .build()
            )
            .thenApply(_ => ())
          future.thenCompose(_ => f)
          loop(t, f)
      }
    }
    loop(requestItems, CompletableFuture.completedFuture(())).join()
  }

  @Benchmark
  def stream_batchWriteItem_single(): Unit = {
    implicit val s   = system
    val requestItems = (for (i <- 1 to 150) yield createWriteRequest(s"pid-2-$i", i))
    val future = streamClient
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(Map("table1" -> requestItems.asJava).asJava)
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

  @Benchmark
  def stream_batchWriteItem_multi(): Unit = {
    implicit val s   = system
    val requestItems = (for (i <- 1 to 150) yield createWriteRequest(s"pid-2-$i", i))
    val future = streamClient
      .batchWriteItemSource(
        BatchWriteItemRequest
          .builder()
          .requestItems(
            Map("table1" -> requestItems.asJava, "table2" -> requestItems.asJava).asJava
          )
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
              "pkey"  -> AttributeValue.builder().s("pid-1").build(),
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
              "pkey"           -> AttributeValue.builder().s("pid-1").build(),
              "skey"           -> AttributeValue.builder().s("0").build(),
              "persistence-id" -> AttributeValue.builder().s("pid-1").build(),
              "sequence-nr"    -> AttributeValue.builder().n("0").build(),
              "value"          -> AttributeValue.builder().s("test").build()
            ).asJava
          )
          .build()
      )
      .runWith(Sink.seq)
    Await.result(future, Duration.Inf)
  }

}
