name := "kafka-examples"
 
version := "0.1"

scalaVersion := "2.11.11"
scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation")

parallelExecution in Test := false

//fork in run := true
// using logback, this is artifact from other SBT based projects
// javaOptions in run ++= Seq(
//  "-Dlog4j.debug=true",
//  "-Dlog4j.configuration=log4j.properties")

//https://github.com/sbt/sbt/issues/3618#issuecomment-424924293
val kafka = "0.10.2.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafka

val logback = "1.2.3"
libraryDependencies += "ch.qos.logback" % "logback-core" % logback
libraryDependencies += "ch.qos.logback" % "logback-classic" % logback


// libraryDependencies += "org.apache.kafka" % "kafka-streams-test-utils" % "2.1.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
