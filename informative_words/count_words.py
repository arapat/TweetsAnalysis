#!/usr/bin/python

# Count the number of words appears in the context

import json
from pyspark import SparkContext

class CountWords:
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "CountUniqueTweets", pyFiles=['countUniqueTweets.py'])

    def gen_rdd(self, tweets_file):
        return self.sc.textFile(tweets_file).map(self.process_tweet)

    def process_tweet(self, raw_tweet):
        try:
            tweet = json.loads(raw_tweet)
            return tweet['text'].split()
        except:
            return 0

    def count_unique_words(self, files):
        if len(files):
            return 0

        all_words = None
        for file_name in files:
            words = self.gen_rdd(file_name)
            if all_words:
                all_words = all_words.union(words)
            else:
                all_words = words

        return all_words.distinct().count()


if __name__ == '__main__':
    dir_path = '/user/arapat/twitter/'
    files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]

    instance = CountWords()
    print "Total words:", instance.count_unique_words(files)

