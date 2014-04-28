
import breeze.linalg.{Vector, SparseVector, squaredDistance}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.json4s._
import org.json4s.native.JsonMethods


object SparkKMeans {

    implicit val formats = DefaultFormats

    def genRdd(tweetsFile: String, sc: SparkContext) = {
        sc.textFile(tweetsFile).map(processTweet)
    }

    def processTweet(rawData: String) : (Int, Array[String]) = {
        try {
            val rawDataArray = rawData.split('\t')
            val rawJson = JsonMethods.parse(rawDataArray(3))
            val uid = (rawJson \ "user" \ "id").extract[Int]
            val tokens = rawDataArray(0).split(' ').map(_.trim)
            return (uid.toInt, tokens)
        } catch {
            case _: Throwable => // broken tweets
                return (0, Array())
        }
    }

    def normalize(user: (Int, (Int, Array[String]))) = {
        val tokenSet = user._2._2.toSet
        val tokenPairs = tokenSet.map(x => (x, user._2._2.count(_ == x)))
            .map(pair => (pair._1, pair._2 / user._2._1.toDouble)).toArray
        (user._1, tokenPairs)
    }

    def toSparse(dict: Map[String, Int])(user: (Int, Array[(String, Double)])): (Int, SparseVector[Double]) = {
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
        // val MIN_POST = 0
        // val MIN_OCCURS = 0

        val rawUsers = files.map(genRdd(_, sc))
        var unionUsers = rawUsers(0)
        for (i <- 1 until rawUsers.length) {
            unionUsers = unionUsers.union(rawUsers(i))
        }

        val userAndTokens = unionUsers
            .map {case (uid, tokens) => (uid, (1, tokens))}
            .reduceByKey {case ((x1, y1), (x2, y2)) => (x1 + x2, y1 ++ y2)}
            .filter {case (uid, (counter, tokens)) => counter >= MIN_POST}

        val allWords = userAndTokens
            .flatMap {case (uid, (counter, tokens)) => tokens.map((_, 1))}
            .reduceByKey(_ + _)
            .filter {case (token, counter) => counter >= MIN_OCCURS}
            .map {case (token, counter) => token}
            .collect().sorted
        val dict = {for (i <- 0 until allWords.length) yield (allWords(i), i)}.toMap

        // Construct features
        val toSparseFunc = toSparse(dict)(_)
        val allVectors = userAndTokens.map(normalize).map(toSparseFunc).cache

        // Clustering (k-means)
        val K = 30
        val CONVERGE = 0.1
        val MAX_ITER = 10
        var tempDist = 1.0
        val kPoints = allVectors.takeSample(withReplacement = false, K, 42)
            .map {case (uid, vector) => vector}
            .toArray

        var iter = 0
        while(tempDist > CONVERGE && iter < MAX_ITER) {

            val closest = allVectors.map {case (uid, p) => (closestPoint(p, kPoints), (p, 1))}

            val pointStats = closest.reduceByKey {case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}

            val newPoints = pointStats.map {pair =>
                (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

            tempDist = 0.0 
            for (i <- 0 until K) {
                tempDist += squaredDistance(kPoints(i), newPoints(i))
            }

            for (newP <- newPoints) {
                kPoints(newP._1) = newP._2
            }

            iter = iter + 1
            println("Finished iteration " + iter + " (delta = " + tempDist + ")")
        }

        println("Final centers:")
        kPoints.foreach(println)
        // for point in kPoints:
        // print "=========="
        // row, col = point.nonzero()
        // for i,j in zip(row, col):
        // print i,j,point[i,j]
        // print "=========="
        // print "w_len =", w_len

        // print '\n\nSize of each group:'
        // closest = all_users.map(closestPoint).cache()
        // for i in range(30):
        //     print "Group %02d" % i
        //     print [t[1][2] for t in closest.filter(lambda (index, others): index == i).collect()]

        // return all_users.count()
    }


    def main(args: Array[String]) {
        val sc = new SparkContext("spark://ion-21-14.sdsc.edu:7077", "ClusteringUsers",
              System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)
        
        val dirPath = "/user/arapat/twitter-tag/"
        // val files =
        //     {for (i <- 1 to 70) yield dirPath + "t%02d".format(i)}
        //     ++ {for (i <- 1 to 85) yield dirPath + "u%02d".format(i)}
        val files = Array(dirPath + "t02d".format(1))
        clusteringUser(sc, files)
    }
}

