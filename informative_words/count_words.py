#!/usr/bin/python

# Count the number of words appears in the context

import json
from pyspark import SparkContext
from operator import add


def gen_rdd(tweets_file):
    return sc.textFile(tweets_file).flatMap(process_tweet)

def process_tweet(raw_tweet):
    try:
        tweet = json.loads(raw_tweet)
        return tweet['text'].split()
    except:
        return []

def count_unique_words(files):
    if len(files) == 0:
        return 0

    all_words = None
    for file_name in files:
        words = gen_rdd(file_name)
        if all_words:
            all_words = all_words.union(words)
        else:
            all_words = words

    return all_words.reduceByKey(add).count()


if __name__ == '__main__':
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "CountWords", pyFiles=['count_words.py'])
    dir_path = '/user/arapat/twitter/'
    # files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    files = [dir_path + 't02']

    print "Total words:", count_unique_words(files)

