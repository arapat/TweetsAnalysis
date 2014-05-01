name := "ClusteringUsers"

version := "0.1"

scalaVersion := "2.10.3"

resolvers ++= Seq(
            "Akka Repository" at "http://repo.akka.io/releases/",
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

libraryDependencies ++= Seq(
            "org.json4s" %% "json4s-native" % "3.2.8",
            "org.apache.spark" %% "spark-core" % "0.9.1", 
            "org.scalanlp" % "breeze_2.10" % "0.8-SNAPSHOT",
            "org.scalanlp" % "breeze-natives_2.10" % "0.8-SNAPSHOT",
            "org.apache.hadoop" % "hadoop-client" % "1.1.1"
)

retrieveManaged := true

