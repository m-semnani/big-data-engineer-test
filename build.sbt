lazy val root = (project in file(".")).
  settings(
    name := "linkit",
    version := "0.1",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("nl.linkit.bigdata.hbase.DangerousDriver")
  )

val dependencyScope = "compile"
val HadoopVersion = "2.7.2"
resolvers := List("Hortonworks Releases" at "http://repo.hortonworks.com/content/repositories/releases/")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.2" % "compile" 
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.2" % "compile" 
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.2" % "compile"
libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11" % "compile"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
