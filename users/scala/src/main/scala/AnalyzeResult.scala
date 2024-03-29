package main.scala

import scala.io.Source

import breeze.linalg.{Vector, SparseVector, squaredDistance}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.json4s._
import org.json4s.native.JsonMethods


object AnalyzeResult {

    implicit val formats = DefaultFormats

    def stringToTuple(raw: String) = {
        val index = raw.slice(raw.indexOf('(') + 1, raw.indexOf(','))
        val value = raw.slice(raw.indexOf(',') + 1, raw.indexOf(')'))
        (index.toInt, value.toDouble)
    }

    def getVectorElements(raw: String) = {
        val trimed = raw.drop(raw.indexOf('(') + 1).dropRight(1)
        val strTuples = trimed.split(", ")
        val tuples = strTuples.map(stringToTuple)
        // tuples.sortBy(_._2)
        tuples
    }

    def getSparseVector(dictSize: Int, vectorElements: Array[(Int, Double)]) = {
        val vector = SparseVector[Double](dictSize)()
        for (i <- 0 until vectorElements.length) {
            vector(vectorElements(i)._1) = vectorElements(i)._2
        }
        vector
    }

    def getCenters(numOfCenters: Int, dictSize: Int) = {
        val filePath = "/oasis/projects/nsf/csd181/arapat/project/twitter/scripts/users/scala/centers.txt"
        val raw = Source.fromFile(filePath).getLines.toArray
        assert(raw.length == numOfCenters)

        val vectorElements = raw.map(getVectorElements)
        (vectorElements.map(getSparseVector(dictSize, _)),
            vectorElements.map(_.sortBy((elm: (Int, Double)) => elm._1)))
    }

    def genRdd(tweetsFile: String, sc: SparkContext) = {
        sc.textFile(tweetsFile).map(processTweet)
    }

    def processTweet(rawData: String) : (Int, Array[String], (String, String)) = {
        try {
            val rawDataArray = rawData.split('\t')
            val rawJson = JsonMethods.parse(rawDataArray(3))
            val uid = (rawJson \ "user" \ "id").extract[Int]
            val uname = (rawJson \ "user" \ "name").extract[String]
            val udescription = (rawJson \ "user" \ "description").extract[String]
            val tokens = rawDataArray(0).split(' ').map(_.trim)
            return (uid.toInt, tokens, (uname, udescription))
        } catch {
            case _: Throwable => // broken tweets
                return (0, Array(), ("", ""))
        }
    }

    def normalize(user: (Int, (Int, Array[String], (String, String)))) = {
        val tokenSet = user._2._2.toSet
        val tokenPairs = tokenSet.map(x => (x, user._2._2.count(_ == x)))
            .map(pair => (pair._1, pair._2 / user._2._1.toDouble)).toArray
        (user._1, tokenPairs)
    }

    def toSparse(dict: Map[String, Int], user: (Int, Array[(String, Double)])): (Int, SparseVector[Double]) = {
        val seq = user._2
            .map(pair => (dict.getOrElse(pair._1, -1), pair._2))
            .filter(elm => elm._1 >= 0).sorted
        val vector: SparseVector[Double] = SparseVector[Double](dict.size)()
        for (i <- 0 until seq.length) {
            vector(seq(i)._1) = seq(i)._2
        }

        (user._1, vector)
    }

    def closestPoint(p: SparseVector[Double], centers: Array[SparseVector[Double]]): Int = {
        var index = 0
        var bestIndex = 0
        var closest = Double.PositiveInfinity

        for (i <- 0 until centers.length) {
            val tempDist = squaredDistance(p, centers(i))
            if (tempDist < closest) {
                closest = tempDist
                bestIndex = i
            }
        }

        bestIndex
    }

    def clusteringUser(sc: SparkContext, files: Array[String]) {
        if (files.length == 0)
            return

        // Ignore users that have fewer than MIN_POST tweets
        val MIN_POST = 7
        val MIN_OCCURS = 10000

        val rawUsers = files.map(genRdd(_, sc))
        var unionUsers = rawUsers(0)
        for (i <- 1 until rawUsers.length) {
            unionUsers = unionUsers.union(rawUsers(i))
        }

        val userAndTokens = unionUsers
            .map {case (uid, tokens, info) => (uid, (1, tokens, info))}
            .reduceByKey(
                (a: (Int, Array[String], (String, String)), b: (Int, Array[String], (String, String)))
                    => (a._1 + b._1, a._2 ++ b._2, a._3), 240)
            .filter {case (uid, (counter, tokens, info)) => counter >= MIN_POST}

        val allWords = userAndTokens
            .flatMap {case (uid, (counter, tokens, info)) => tokens.map((_, 1))}
            .reduceByKey(_ + _, 240)
            .filter {case (token, counter) => counter >= MIN_OCCURS}
            .map {case (token, counter) => token}
            .collect().sorted
        val dict = {for (i <- 0 until allWords.length) yield (allWords(i), i)}.toMap
        val dictReverse = {for (i <- 0 until allWords.length) yield (i, allWords(i))}.toMap

        // Construct features
        val allVectors = userAndTokens.map(normalize).map(toSparse(dict, _))
        val allUsers = userAndTokens.map {
            case (uid, (counter, tokens, info)) => (uid, (counter, info))
        }.cache

        // Prediction (k-means)
        val K = 30
        val kPointsPair = getCenters(K, dict.size)
        val kPoints = kPointsPair._1
        val kPointsElements = kPointsPair._2
        val closest = allVectors.map {case (uid, p) => (closestPoint(p, kPoints), Array(uid))}
        val prediction = closest.reduceByKey(_ ++ _).collect().sortBy(_._1)

        for (i <- 0 until K) {
            println("========================")

            println("Top 100 Frequent Tokens")
            kPointsElements(i).slice(0, 100)
                  .map((tokenPair: (Int, Double))
                      => (dictReverse(tokenPair._1), tokenPair._2))
                  .foreach((tokenPair: (String, Double))
                      => println(tokenPair._1 + ' ' + tokenPair._2))

            println("Top 100 Active Users")
            val targetUsers = allUsers.filter {
                user: (Int, (Int, (String, String)))
                    => prediction(i)._2.find((uid: Int) => uid == user._1).isEmpty == false
            }
            val usersInfo = targetUsers.map((user: (Int, (Int, (String, String))))
                    => (user._2._1, user._2._2)).collect()
            usersInfo.sorted.slice(0, 100).foreach(println)
        }
    }


    def main(args: Array[String]) {
        val conf = new SparkConf()
            .setMaster("spark://ion-21-14.sdsc.edu:7077")
            .setAppName("ClusteringUsers")
            .setSparkHome(System.getenv("SPARK_HOME"))
            .setJars(Array("target/scala-2.10/clusteringusers_2.10-0.1.jar",
                "lib_managed/jars/org.json4s/json4s-core_2.10/json4s-core_2.10-3.2.8.jar",
                "lib_managed/jars/org.json4s/json4s-native_2.10/json4s-native_2.10-3.2.8.jar",
                "lib_managed/jars/org.json4s/json4s-ast_2.10/json4s-ast_2.10-3.2.8.jar",
                "lib_managed/jars/org.scalanlp/breeze_2.10/breeze_2.10-0.8-SNAPSHOT.jar",
                "lib_managed/jars/org.scalanlp/breeze-natives_2.10/breeze-natives_2.10-0.8-SNAPSHOT.jar"))
            .set("spark.executor.memory", "30g")
        val sc = new SparkContext(conf)

        val dirPath = "hdfs://ion-21-14.ibnet0:54310/user/arapat/twitter-tag/"
        val files1 = {for (i <- 1 to 70) yield dirPath + "t%02d".format(i)}.toArray
        val files2 = {for (i <- 1 to 85) yield dirPath + "u%02d".format(i)}.toArray
        val files = (files1 ++ files2) // Array(dirPath + "t%02d".format(2))
        clusteringUser(sc, files)
    }
}

