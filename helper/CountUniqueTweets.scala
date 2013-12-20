import scala.util.parsing.json._

import org.apache.spark.SparkContext
import org.apache.spark.util.Vector
import org.apache.spark.SparkContext._

/**
 * Count unique tweets
 */
object CountUniqueTweets {

  def process_tweet(raw_tweet: String) : Option[Any] = {
      return JSON.parseFull(raw_tweet)     
  }
      

  def main(args: Array[String]) {
    val sc = new SparkContext("ion-21-14.local:7077", "CountUniqueTweets",
      System.getenv("SPARK_HOME"), Seq())

    var dir_path = "/oasis/projects/nsf/csd181/arapat/project/twitter/raw/comb_election1/"
    var all_tweets = sc.textFile(dir_path + "t01").map(process_tweet).distinct()
    for (i <- 2 until 71) {
      var file_name = dir_path + "t" + "%02d".format(i)
      var tweets = sc.textFile(file_name).map(process_tweet).distinct()
      all_tweets = all_tweets.union(gen_rdd(file_name)).distinct()
    }

    println("Final result: " + all_tweets.count())
    System.exit(0)
  }
}

