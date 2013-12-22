import os
import json
from operator import add
from pyspark import SparkContext


def gen_rdd(tweets_file):
    return sc.textFile(tweets_file).map(process_tweet)

def process_tweet(raw_tweet):
    try:
        tweet = json.loads(raw_tweet)
        return tweet['id']
    except:
        return 0

if __name__ == "__main__":
  sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "CountUniqueTweets", pyFiles=['countUniqueTweets.py'])

  dir_path = 'twitter/election1'
  all_tweets = gen_rdd(os.path.join(dir_path, 't01'))
  for fnum in range(2, 71):
      file_name = os.path.join(dir_path, 't' + '%02d' % fnum)
      all_tweets = all_tweets.union(gen_rdd(file_name))
  print "Total unique tweets: ", all_tweets.distinct().count()-1

