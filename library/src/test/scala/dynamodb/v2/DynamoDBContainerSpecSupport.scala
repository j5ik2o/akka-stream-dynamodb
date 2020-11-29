package dynamodb.v2

import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, ForAllTestContainer}
import org.scalatest.Suite

trait DynamoDBContainerSpecSupport extends DynamoDBContainerHelper with ForAllTestContainer {
  this: Suite =>

  override val container: FixedHostPortGenericContainer = dynamoDbLocalContainer

}
