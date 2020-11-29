val AkkaVersion                  = "2.6.10"
val AwsSdkDynamoDBVersion        = "2.13.75"
val scalaCollectionCompatVersion = "2.1.6"
val testcontainersScalaVersion   = "0.38.5"
val awsSdkV1Version              = "1.11.903"
val slf4jVersion                 = "1.7.30"
val logbackVersion               = "1.2.3"

import sbt.Keys._

val scala211Version = "2.11.12"
val scala212Version = "2.12.10"
val scala213Version = "2.13.4"

lazy val baseSettings = Seq(
  organization := "com.github.j5ik2o",
  scalaVersion := scala213Version,
  crossScalaVersions := Seq(scala211Version, scala212Version, scala213Version),
  scalacOptions ++= (
    Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:_",
      "-Ydelambdafy:method",
      "-target:jvm-1.8"
    ) ++ crossScalacOptions(scalaVersion.value)
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("snapshots"),
    Resolver.sonatypeRepo("releases")
  ),
  parallelExecution in Test := false,
  scalafmtOnCompile in ThisBuild := true
)

def crossScalacOptions(scalaVersion: String): Seq[String] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2L, scalaMajor)) if scalaMajor >= 12 =>
      Seq.empty
    case Some((2L, scalaMajor)) if scalaMajor <= 11 =>
      Seq("-Yinline-warnings")
  }

lazy val deploySettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/j5ik2o/akka-stream-dynamodb</url>
      <licenses>
        <license>
          <name>The MIT License</name>
          <url>http://opensource.org/licenses/MIT</url>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:j5ik2o/akka-stream-dynamodb.git</url>
        <connection>scm:git:github.com/j5ik2o/akka-stream-dynamodb</connection>
        <developerConnection>scm:git:git@github.com:j5ik2o/akka-stream-dynamodb.git</developerConnection>
      </scm>
      <developers>
        <developer>
          <id>j5ik2o</id>
          <name>Junichi Kato</name>
        </developer>
      </developers>
  },
  publishTo := sonatypePublishToBundle.value,
  credentials := {
    val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
    val gpgCredentials = (baseDirectory in LocalRootProject).value / ".gpgCredentials"
    Credentials(ivyCredentials) :: Credentials(gpgCredentials) :: Nil
  }
)

val test = (project in file("test"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-stream-dynamodb-test",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "software.amazon.awssdk"  % "dynamodb"                % AwsSdkDynamoDBVersion,
      "com.typesafe.akka"      %% "akka-stream"             % AkkaVersion,
      "com.amazonaws"           % "aws-java-sdk-dynamodb"   % awsSdkV1Version,
      "com.dimafeng"           %% "testcontainers-scala"    % testcontainersScalaVersion
    )
  )

val library = (project in file("library"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-stream-dynamodb",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      "software.amazon.awssdk"  % "dynamodb"                % AwsSdkDynamoDBVersion,
      "com.typesafe.akka"      %% "akka-stream"             % AkkaVersion,
      "com.typesafe.akka"      %% "akka-slf4j"              % AkkaVersion,
      "com.amazonaws"           % "aws-java-sdk-dynamodb"   % awsSdkV1Version,
      "com.typesafe.akka"      %% "akka-testkit"            % AkkaVersion,
      "com.dimafeng"           %% "testcontainers-scala"    % testcontainersScalaVersion % Test,
      "org.scalatest"          %% "scalatest"               % "3.1.1"                    % Test,
      "org.scalatestplus"      %% "scalacheck-1-14"         % "3.1.1.0"                  % Test,
      "org.slf4j"               % "slf4j-api"               % slf4jVersion               % Test,
      "ch.qos.logback"          % "logback-classic"         % logbackVersion             % Test
    )
  )
  .dependsOn(test % "test->compile")

lazy val benchmark = (project in file("benchmark"))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-stream-dynamodb-benchmark",
    skip in publish := true,
    libraryDependencies ++= Seq(
    )
  )
  .enablePlugins(JmhPlugin)
  .dependsOn(library, test)

val root = (project in file("."))
  .settings(baseSettings)
  .settings(deploySettings)
  .settings(
    name := "akka-stream-dynamodb-root",
    skip in publish := true
  )
  .aggregate(library, benchmark)
