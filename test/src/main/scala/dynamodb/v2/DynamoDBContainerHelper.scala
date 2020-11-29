package dynamodb.v2

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import org.slf4j.LoggerFactory
import org.testcontainers.containers.wait.strategy.Wait

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait DynamoDBContainerHelper {
  private val logger = LoggerFactory.getLogger(getClass)

  protected lazy val region: Regions = Regions.AP_NORTHEAST_1

  protected lazy val accessKeyId: String = "x"

  protected lazy val secretAccessKey: String = "x"

  protected lazy val dynamoDBPort: Int = RandomPortUtil.temporaryServerPort()

  protected lazy val dynamoDBEndpoint: String = s"http://127.0.0.1:$dynamoDBPort"

  protected lazy val dynamoDBImageVersion: String = "1.13.4"

  protected lazy val dynamoDBImageName: String = s"amazon/dynamodb-local:$dynamoDBImageVersion"

  protected lazy val dynamoDbLocalContainer: FixedHostPortGenericContainer =
    FixedHostPortGenericContainer(
      dynamoDBImageName,
      exposedHostPort = dynamoDBPort,
      exposedContainerPort = 8000,
      command = Seq("-Xmx256m", "-jar", "DynamoDBLocal.jar", "-dbPath", ".", "-sharedDb"),
      waitStrategy = Wait.forListeningPort()
    )

  protected lazy val dynamoDBClient: AmazonDynamoDB = {
    AmazonDynamoDBClientBuilder
      .standard()
      .withCredentials(
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(accessKeyId, secretAccessKey)
        )
      )
      .withEndpointConfiguration(
        new EndpointConfiguration(dynamoDBEndpoint, region.getName)
      )
      .build()
  }

  protected val waitIntervalForDynamoDBLocal: FiniteDuration = 500 milliseconds

  protected val MaxCount = 10

  protected def waitDynamoDBLocal(tableNames: Seq[String]): Unit = {
    var isWaken: Boolean = false
    var counter          = 0
    while (counter < MaxCount && !isWaken) {
      try {
        val listTablesResult = dynamoDBClient.listTables(2)
        if (tableNames.forall(s => listTablesResult.getTableNames.asScala.contains(s))) {
          println("finish")
          isWaken = true
        } else {
          println("waiting...")
          Thread.sleep(1500)
        }
      } catch {
        case _: ResourceNotFoundException =>
          counter += 1
          Thread.sleep(waitIntervalForDynamoDBLocal.toMillis)
      }
    }
  }

  def deleteTable(tableNames: Seq[String]): Unit =
    synchronized {
      Thread.sleep(500)
      tableNames.foreach(deleteTable)
      Thread.sleep(500)
    }

  def createTable(tableNames: Seq[String]): Unit =
    synchronized {
      Thread.sleep(500)
      tableNames.foreach(createTable)
      waitDynamoDBLocal(tableNames)
    }

  private def deleteTable(tableName: String): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (listTablesResult.getTableNames.asScala.exists(_.contains(tableName)))
      dynamoDBClient.deleteTable(tableName)
    val result = dynamoDBClient.listTables(2)
    require(!result.getTableNames.asScala.exists(_.contains(tableName)))
  }

  protected def createTable(tableName: String): Unit = {
    val listTablesResult = dynamoDBClient.listTables(2)
    if (!listTablesResult.getTableNames.asScala.exists(_.contains(tableName))) {
      val createRequest = new CreateTableRequest()
        .withTableName(tableName)
        .withAttributeDefinitions(
          Seq(
            new AttributeDefinition()
              .withAttributeName("pkey")
              .withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition()
              .withAttributeName("skey")
              .withAttributeType(ScalarAttributeType.S)
          ).asJava
        )
        .withKeySchema(
          Seq(
            new KeySchemaElement().withAttributeName("pkey").withKeyType(KeyType.HASH),
            new KeySchemaElement().withAttributeName("skey").withKeyType(KeyType.RANGE)
          ).asJava
        )
        .withProvisionedThroughput(
          new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L)
        )
      val createResponse = dynamoDBClient.createTable(createRequest)
      require(createResponse.getSdkHttpMetadata.getHttpStatusCode == 200)
    }
  }
}
