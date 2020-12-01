package dynamodb.v2

import java.net.URI

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{KeysAndAttributes, _}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class DynamoDBStreamClientSpec
    extends TestKit(ActorSystem("test"))
    with AnyFreeSpecLike
    with DynamoDBContainerSpecSupport
    with ScalaFutures {
  private implicit val pc: PatienceConfig = PatienceConfig(30 seconds, 1 seconds)

  var client: DynamoDbAsyncClient        = _
  var streamClient: DynamoDBStreamClient = _

  override def afterStart(): Unit = {
    super.afterStart()
    createTable(Seq("table1", "table2"))
    client = DynamoDbAsyncClient
      .builder()
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
      )
      .endpointOverride(URI.create(dynamoDBEndpoint))
      .build()
    streamClient = new DynamoDBStreamClient(client)
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

  "DynamoDBStreamClient" - {
    "putItem" in {
      streamClient
        .putItemSource(
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
        .runWith(Sink.seq)
        .futureValue
    }
    "batchWriteItem" in {
      val requestItems =
        (for (i <- 1 to 100) yield createWriteRequest(s"batch-write-pid-$i", i))
      streamClient
        .batchWriteItemSource(
          BatchWriteItemRequest
            .builder()
            .requestItems(Map("table1" -> requestItems.asJava).asJava)
            .build()
        )
        .runWith(Sink.seq)
        .futureValue
    }
    "batchGetItem" in {
      val requestItems =
        (for (i <- 1 to 100) yield createWriteRequest(s"batch-write-pid-$i", i))
      streamClient
        .batchWriteItemSource(
          BatchWriteItemRequest
            .builder()
            .requestItems(Map("table1" -> requestItems.asJava).asJava)
            .build()
        )
        .fold(Vector.empty[BatchWriteItemResponse]) { _ :+ _ }
        .flatMapConcat { responses =>
          val keys =
            for (i <- 1 to 100)
              yield Map(
                "pkey" -> AttributeValue.builder().s(s"batch-write-pid-$i").build(),
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
          streamClient.batchGetItemSource(request)
        }
        .runWith(Sink.seq)
        .futureValue

    }
  }

}
