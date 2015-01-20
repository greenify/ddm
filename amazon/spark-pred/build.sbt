name := "Amazon Predictor"
 
version := "1.0"
 
scalaVersion := "2.11.5"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.2.0"
 
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

mainClass in Compile := Some("AmazonPred")
