package dynamodb.v2

import java.util.concurrent.CompletableFuture

import akka.NotUsed
import akka.japi.function
import akka.stream.javadsl.{Flow => JavaFlow}
import akka.stream.scaladsl.{Concat, Flow, Source}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

object DynamoDBStreamClient {
  val BatchGetItemMaxSize   = 100
  val BatchWriteItemMaxSize = 25
}

object FlowMode extends Enumeration {
  val Java, ScalaJDK, ScalaCompat = Value
}
object FlowModeWithPublisher extends Enumeration {
  val Java, ScalaJDK, ScalaCompat, Publisher = Value
}

final class DynamoDBStreamClient(
    client: DynamoDbAsyncClient,
    putItemFlowMode: FlowMode.Value = FlowMode.Java,
    getItemFlowMode: FlowMode.Value = FlowMode.Java,
    deleteItemFlowMode: FlowMode.Value = FlowMode.Java,
    batchGetItemFlowMode: FlowModeWithPublisher.Value = FlowModeWithPublisher.Java,
    batchWriteItemFlowMode: FlowMode.Value = FlowMode.Java,
    queryFlowMode: FlowModeWithPublisher.Value = FlowModeWithPublisher.Java,
    scanFlowMode: FlowModeWithPublisher.Value = FlowModeWithPublisher.Java
) {
  import DynamoDBStreamClient._

  def putItemSource(request: PutItemRequest): Source[PutItemResponse, NotUsed] =
    Source.single(request).via(putItemFlow)

  def putItemFlow: Flow[PutItemRequest, PutItemResponse, NotUsed] = {
    putItemFlowMode match {
      case FlowMode.Java =>
        JavaFlow
          .create[PutItemRequest]()
          .mapAsync(
            1,
            new function.Function[PutItemRequest, CompletableFuture[PutItemResponse]] {
              override def apply(param: PutItemRequest): CompletableFuture[PutItemResponse] =
                client.putItem(param)
            }
          )
          .asScala
      case FlowMode.ScalaJDK =>
        import scala.jdk.FutureConverters._
        Flow[PutItemRequest].mapAsync(1)(client.putItem(_).asScala)
      case FlowMode.ScalaCompat =>
        import scala.compat.java8.FutureConverters._
        Flow[PutItemRequest].mapAsync(1)(client.putItem(_).toScala)
    }
  }

  def getItemSource(request: GetItemRequest): Source[GetItemResponse, NotUsed] =
    Source.single(request).via(getItemFlow)

  def getItemFlow: Flow[GetItemRequest, GetItemResponse, NotUsed] = {
    getItemFlowMode match {
      case FlowMode.Java =>
        JavaFlow
          .create[GetItemRequest]()
          .mapAsync(
            1,
            new function.Function[GetItemRequest, CompletableFuture[GetItemResponse]] {
              override def apply(param: GetItemRequest): CompletableFuture[GetItemResponse] =
                client.getItem(param)
            }
          )
          .asScala
      case FlowMode.ScalaJDK =>
        import scala.jdk.FutureConverters._
        Flow[GetItemRequest].mapAsync(1)(client.getItem(_).asScala)
      case FlowMode.ScalaCompat =>
        import scala.compat.java8.FutureConverters._
        Flow[GetItemRequest].mapAsync(1)(client.getItem(_).toScala)
    }
  }

  def deleteItemSource(request: DeleteItemRequest): Source[DeleteItemResponse, NotUsed] =
    Source.single(request).via(deleteItemFlow)

  def deleteItemFlow: Flow[DeleteItemRequest, DeleteItemResponse, NotUsed] = {
    deleteItemFlowMode match {
      case FlowMode.Java =>
        JavaFlow
          .create[DeleteItemRequest]()
          .mapAsync(
            1,
            new function.Function[DeleteItemRequest, CompletableFuture[DeleteItemResponse]] {
              override def apply(param: DeleteItemRequest): CompletableFuture[DeleteItemResponse] =
                client.deleteItem(param)
            }
          )
          .asScala
      case FlowMode.ScalaJDK =>
        import scala.jdk.FutureConverters._
        Flow[DeleteItemRequest].mapAsync(1)(client.deleteItem(_).asScala)
      case FlowMode.ScalaCompat =>
        import scala.compat.java8.FutureConverters._
        Flow[DeleteItemRequest].mapAsync(1)(client.deleteItem(_).toScala)
    }
  }

  private def internalBatchGetItemFlow: Flow[BatchGetItemRequest, BatchGetItemResponse, NotUsed] = {
    batchGetItemFlowMode match {
      case FlowModeWithPublisher.Java =>
        JavaFlow
          .create[BatchGetItemRequest]()
          .mapAsync(
            1,
            new function.Function[BatchGetItemRequest, CompletableFuture[BatchGetItemResponse]] {
              override def apply(
                  param: BatchGetItemRequest
              ): CompletableFuture[BatchGetItemResponse] =
                client.batchGetItem(param)
            }
          )
          .asScala
      case FlowModeWithPublisher.ScalaJDK =>
        import scala.jdk.FutureConverters._
        Flow[BatchGetItemRequest].mapAsync(1)(client.batchGetItem(_).asScala)
      case FlowModeWithPublisher.ScalaCompat =>
        import scala.compat.java8.FutureConverters._
        Flow[BatchGetItemRequest].mapAsync(1)(client.batchGetItem(_).toScala)
      case FlowModeWithPublisher.Publisher =>
        Flow[BatchGetItemRequest].flatMapConcat { request =>
          Source.fromPublisher(client.batchGetItemPaginator(request))
        }
    }
  }

  private def internalAwareBatchGetItemFlow(
      shardSize: Int
  ): Flow[BatchGetItemRequest, BatchGetItemResponse, NotUsed] =
    Flow[BatchGetItemRequest].flatMapConcat { request =>
      if (
        request.requestItems().asScala.exists {
          case (_, items) => items.keys().size > BatchGetItemMaxSize
        }
      ) {
        Source(request.requestItems().asScala.toMap)
          .groupBy(shardSize, { case (_, v) => math.abs(v.##) % shardSize })
          .mapConcat {
            case (k, v) =>
              v.keys.asScala.toVector.map((k, _))
          }
          .grouped(BatchGetItemMaxSize)
          .map { items =>
            val tableName = items.head._1
            val keys      = items.map(_._2)
            val params    = KeysAndAttributes.builder().keys(keys.asJava).build()

            request.toBuilder.requestItems(Map(tableName -> params).asJava).build()
          }
          .via(internalBatchGetItemFlow)
          .mergeSubstreams
      } else
        Source.single(request).via(internalBatchGetItemFlow)
    }

  def batchGetItemSource(
      request: BatchGetItemRequest,
      shardSize: Int = Int.MaxValue
  ): Source[BatchGetItemResponse, NotUsed] =
    Source.single(request).via(batchGetItemFlow(shardSize))

  def batchGetItemFlow(
      shardSize: Int = Int.MaxValue
  ): Flow[BatchGetItemRequest, BatchGetItemResponse, NotUsed] = {
    def loop(
        acc: Source[BatchGetItemResponse, NotUsed]
    ): Flow[BatchGetItemRequest, BatchGetItemResponse, NotUsed] =
      Flow[BatchGetItemRequest].flatMapConcat { request =>
        Source.single(request).via(internalAwareBatchGetItemFlow(shardSize)).flatMapConcat {
          response =>
            val unprocessedKeys = Option(
              response
                .unprocessedKeys()
            ).map(_.asScala.toMap).getOrElse(Map.empty)
            if (response.hasUnprocessedKeys && unprocessedKeys.nonEmpty) {
              val nextRequest =
                request.toBuilder
                  .requestItems(unprocessedKeys.asJava)
                  .build()
              Source
                .single(nextRequest)
                .via(loop(Source.combine(acc, Source.single(response))(Concat(_))))
            } else
              Source.combine(acc, Source.single(response))(Concat(_))
        }
      }
    loop(Source.empty)
  }

  private def internalBatchWriteItemFlow
      : Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] = {
    batchWriteItemFlowMode match {
      case FlowMode.Java =>
        JavaFlow
          .create[BatchWriteItemRequest]()
          .mapAsync(
            1,
            new function.Function[
              BatchWriteItemRequest,
              CompletableFuture[BatchWriteItemResponse]
            ] {
              override def apply(
                  param: BatchWriteItemRequest
              ): CompletableFuture[BatchWriteItemResponse] =
                client.batchWriteItem(param)
            }
          )
          .asScala
      case FlowMode.ScalaJDK =>
        import scala.jdk.FutureConverters._
        Flow[BatchWriteItemRequest].mapAsync(1)(client.batchWriteItem(_).asScala)
      case FlowMode.ScalaCompat =>
        import scala.compat.java8.FutureConverters._
        Flow[BatchWriteItemRequest].mapAsync(1)(client.batchWriteItem(_).toScala)
    }
  }

  private def internalAwareBatchWriteItemFlow(
      shardSize: Int
  ): Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] = {
    Flow[BatchWriteItemRequest].flatMapConcat { request =>
      if (
        request.requestItems().asScala.exists {
          case (_, items) => items.size > BatchWriteItemMaxSize
        }
      ) {
        Source(request.requestItems().asScala.toMap)
          .groupBy(shardSize, { case (_, v) => math.abs(v.##) % shardSize })
          .mapConcat { case (k, v) => v.asScala.toVector.map((k, _)) }
          .grouped(BatchWriteItemMaxSize)
          .map { items =>
            val tableName    = items.head._1
            val requestItems = items.map(_._2)
            request.toBuilder.requestItems(Map(tableName -> requestItems.asJava).asJava).build()
          }
          .via(internalBatchWriteItemFlow)
          .mergeSubstreams
      } else
        Source.single(request).via(internalBatchWriteItemFlow)
    }
  }

  def batchWriteItemSource(
      request: BatchWriteItemRequest,
      shardSize: Int = Int.MaxValue
  ): Source[BatchWriteItemResponse, NotUsed] =
    Source.single(request).via(batchWriteItemFlow(shardSize))

  def batchWriteItemFlow(
      shardSize: Int = Int.MaxValue
  ): Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] = {
    def loop(
        acc: Source[BatchWriteItemResponse, NotUsed]
    ): Flow[BatchWriteItemRequest, BatchWriteItemResponse, NotUsed] =
      Flow[BatchWriteItemRequest].flatMapConcat { request =>
        Source.single(request).via(internalAwareBatchWriteItemFlow(shardSize)).flatMapConcat {
          response =>
            val unprocessedItems = Option(
              response
                .unprocessedItems()
            ).map(_.asScala.toMap)
              .map(_.map {
                case (k, v) => (k, v.asScala.toVector)
              })
              .getOrElse(Map.empty)
            if (response.hasUnprocessedItems && unprocessedItems.nonEmpty) {
              val nextRequest =
                request.toBuilder
                  .requestItems(unprocessedItems.map { case (k, v) => (k, v.asJava) }.asJava)
                  .build()
              Source
                .single(nextRequest)
                .via(loop(Source.combine(acc, Source.single(response))(Concat(_))))
            } else
              Source.combine(acc, Source.single(response))(Concat(_))
        }
      }
    loop(Source.empty)
  }

  private def internalQueryFlow: Flow[QueryRequest, QueryResponse, NotUsed] = {
    queryFlowMode match {
      case FlowModeWithPublisher.Java =>
        JavaFlow
          .create[QueryRequest]()
          .mapAsync(
            1,
            new function.Function[QueryRequest, CompletableFuture[QueryResponse]] {
              override def apply(param: QueryRequest): CompletableFuture[QueryResponse] =
                client.query(param)
            }
          )
          .asScala
      case FlowModeWithPublisher.ScalaJDK =>
        import scala.jdk.FutureConverters._
        Flow[QueryRequest].mapAsync(1)(client.query(_).asScala)
      case FlowModeWithPublisher.ScalaCompat =>
        import scala.compat.java8.FutureConverters._
        Flow[QueryRequest].mapAsync(1)(client.query(_).toScala)
      case FlowModeWithPublisher.Publisher =>
        Flow[QueryRequest].flatMapConcat { request =>
          Source.fromPublisher(client.queryPaginator(request))
        }
    }
  }

  def queryFlow(maxOpt: Option[Long]): Flow[QueryRequest, QueryResponse, NotUsed] =
    Flow[QueryRequest].flatMapConcat(querySource(_, maxOpt))

  def querySource(
      queryRequest: QueryRequest,
      maxOpt: Option[Long]
  ): Source[QueryResponse, NotUsed] = {
    def loop(
        queryRequest: QueryRequest,
        maxOpt: Option[Long],
        lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
        acc: Source[QueryResponse, NotUsed] = Source.empty,
        count: Long = 0
    ): Source[QueryResponse, NotUsed] = {
      val newQueryRequest = lastEvaluatedKey match {
        case None =>
          queryRequest
        case Some(_) =>
          queryRequest.toBuilder.exclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull).build()
      }
      Source
        .single(newQueryRequest)
        .via(internalQueryFlow)
        .flatMapConcat { response =>
          val lastEvaluatedKey =
            Option(response.lastEvaluatedKey).map(_.asScala.toMap).getOrElse(Map.empty)
          val combinedSource = Source.combine(acc, Source.single(response))(Concat(_))
          if (
            response.hasLastEvaluatedKey && maxOpt.fold(true) { max =>
              (count + response.count()) < max
            }
          ) {
            loop(
              queryRequest,
              maxOpt,
              Some(lastEvaluatedKey),
              combinedSource,
              count + response.count()
            )
          } else
            combinedSource
        }
    }
    loop(queryRequest, maxOpt)
  }

  private def internalScanFlow: Flow[ScanRequest, ScanResponse, NotUsed] = {
    scanFlowMode match {
      case FlowModeWithPublisher.Java =>
        JavaFlow
          .create[ScanRequest]()
          .mapAsync(
            1,
            new function.Function[ScanRequest, CompletableFuture[ScanResponse]] {
              override def apply(param: ScanRequest): CompletableFuture[ScanResponse] =
                client.scan(param)
            }
          )
          .asScala
      case FlowModeWithPublisher.ScalaJDK =>
        import scala.jdk.FutureConverters._
        Flow[ScanRequest].mapAsync(1)(client.scan(_).asScala)
      case FlowModeWithPublisher.ScalaCompat =>
        import scala.compat.java8.FutureConverters._
        Flow[ScanRequest].mapAsync(1)(client.scan(_).toScala)
      case FlowModeWithPublisher.Publisher =>
        Flow[ScanRequest].flatMapConcat { request =>
          Source.fromPublisher(client.scanPaginator(request))
        }
    }
  }

  def scanFlow(maxOpt: Option[Long]): Flow[ScanRequest, ScanResponse, NotUsed] =
    Flow[ScanRequest].flatMapConcat(scanSource(_, maxOpt))

  def scanSource(
      scanRequest: ScanRequest,
      maxOpt: Option[Long]
  ): Source[ScanResponse, NotUsed] = {
    def loop(
        scanRequest: ScanRequest,
        maxOpt: Option[Long],
        lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
        acc: Source[ScanResponse, NotUsed] = Source.empty,
        count: Long = 0
    ): Source[ScanResponse, NotUsed] = {
      val newScanRequest = lastEvaluatedKey match {
        case None =>
          scanRequest
        case Some(_) =>
          scanRequest.toBuilder.exclusiveStartKey(lastEvaluatedKey.map(_.asJava).orNull).build()
      }
      Source
        .single(newScanRequest)
        .via(internalScanFlow)
        .flatMapConcat { response =>
          val lastEvaluatedKey =
            Option(response.lastEvaluatedKey).map(_.asScala.toMap).getOrElse(Map.empty)
          val combinedSource = Source.combine(acc, Source.single(response))(Concat(_))
          if (
            response.hasLastEvaluatedKey && maxOpt.fold(true) { max =>
              (count + response.count()) < max
            }
          ) {
            loop(
              scanRequest,
              maxOpt,
              Some(lastEvaluatedKey),
              combinedSource,
              count + response.count()
            )
          } else
            combinedSource
        }
    }
    loop(scanRequest, maxOpt)
  }

}
