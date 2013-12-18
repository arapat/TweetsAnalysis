import os
import json
from operator import add
from pyspark import SparkContext

sc = SparkContext("ion-21-14.local:7077", "Count Unique Tweets", pyFiles=['countUniqueTweets.py'])

dir_path = '/oasis/projects/nsf/csd181/arapat/project/twitter/raw/comb_election1'
all_tweets = gen_rdd(os.path/join(dir_path, 't01')
for fnum in range(2, 71):
    file_name = os.path/join(dir_path, 't' + '%02d' % fnum)
    all_tweets = all_tweets.join(gen_rdd(file_name))
print "Total unique tweets: ", all_tweets.count()-1


def gen_rdd(tweets_file):
    return sc.textFile(file_name).map(process_tweet).distinct()

def process_tweet(raw_tweet):
    try:
        tweet = json.loads(raw_tweet)
        return tweet['id']
    except:
        return 0

