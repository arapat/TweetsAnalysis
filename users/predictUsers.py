# Note that the add operation in K-means algorithm is defined WITHOUT factors

import json
import os
import sys
from operator import add
from pyspark import SparkContext

from nltk.corpus import stopwords
import numpy as np
from scipy.sparse import csc_matrix

def gen_rdd(tweets_file):
    return sc.textFile(tweets_file).map(process_tweet)


def process_tweet(raw_data):
    try:
        raw_data = raw_data.split('\t')
        raw_json = json.loads(raw_data[3])
        uid = raw_json['user']['id']
        text = [w.strip() for w in raw_data[0].split()]
        return (uid, (1, text))
    except:
        return (0, (0, []))


def predict_users(files):

    w_dict = None
    w_len = 0
    kPoints = None

    sqsum = lambda v: np.sum([d * d for d in v.data])

    def get_centers(K):
        file_path = ""
        f = open("result-prt.txt")
        for i in range(3):
            f.readline()
        separate = f.readline().strip()
        centers = []
        for i in range(30):
            row = []
            data = []
            raw = f.readline()
            while raw.strip() != separate:
                raw = raw.split()
                row.append(int(raw[0]))
                data.append(float(raw[2]))
                raw = f.readline()
            centers.append(csc_matrix((data, (row, [0] * len(row))), shape=(w_len, 1)))
            f.readline()
        return centers


    def normalize(user):
        uid = user[0]
        counter = float(user[1][0])
        tokens = sorted(user[1][1])

        vec = []
        tp = (tokens[0], 1)
        for i in range(1,len(tokens)):
            if tp[0] == tokens[i]:
                tp = (tp[0], tp[1] + 1)
            else:
                vec.append(tp)
                tp = (tokens[i], 1)
        vec.append(tp)
        vec = map(lambda (token, weight): (token, weight / counter), vec)
        return (uid, vec)


    def to_sparse(user):
        uid = user[0]
        vec = user[1]

        row = []
        data = []
        for i in range(len(vec)):
            idx = None
            try:
                idx = w_dict[vec[i][0]]
            except:
                # Do nothing: word appears only once
                pass
            if idx:
                row.append(idx)
                data.append(vec[i][1])

        return (uid, csc_matrix((data, (row, [0] * len(row))), shape=(w_len, 1)))


    def closestPoint(user):
        uid = user[0]
        vec = user[1]
        bestIndex = 0
        closest = float("+inf")
        for i in range(len(kPoints)):
            bestIndex = i
            tempDist = sqsum(vec - kPoints[i])
            if tempDist < closest:
                closest = tempDist
                bestIndex = i
        return (uid, bestIndex)


    if len(files) == 0:
        return 0

    # Ignore users that have fewer than MIN_POST tweets
    MIN_POST = 7
    MIN_OCCURS = 50

    # For each account, put all the tokens of its tweets into one vector
    all_users = None
    for file_name in files:
        users = gen_rdd(file_name)
        if all_users:
            all_users = all_users.union(users)
        else:
            all_users = users

    # Normalize the vector
    all_users = all_users.reduceByKey( \
            lambda a, b: (a[0] + b[0], a[1] + b[1]), numPartitions = 840) \
            .filter(lambda (uid, (counter, tokens)): counter >= MIN_POST) \
            .cache()

    # 4606200 words
    all_words = all_users.flatMap(lambda (uid, (counter, tokens)): [(w, 1) for w in tokens]) \
            .reduceByKey(add, numPartitions=840) \
            .filter(lambda (w, count): count >= MIN_OCCURS) \
            .map(lambda (w, count): w).collect()
    w_len = len(all_words)
    w_dict = {all_words[i]: i for i in range(len(all_words))}

    # Construct features
    vec_users = all_users.map(normalize).map(to_sparse)

    # Predicting (k-means)
    K = 30
    kPoints = get_centers(K)

    closest = vec_users.map(closestPoint).join(all_users).cache()
    all_users.unpersist()

    print "Top 1000 active users"
    for i in range(30):
        print "===== Center %2d =====" % i
        print closest.filter(lambda (uid, cid, (counter, tokens)): cid == i) \
                .map(lambda (uid, cid, (counter, tokens)): (counter, uid)) \
                .sortByKey(False, numPartitions = 840) \
                .take(1000)

    stop_words = stopwords.words('english')
    print "\n\nTop 1000 frequently-used words"
    for i in range(30):
        print "===== Center %2d =====" % i
        print closest.filter(lambda (uid, cid, (counter, tokens)): cid == i) \
                .flatMap(lambda (uid, cid, (counter, tokens)): [(w, 1) for w in tokens if w not in stop_words]) \
                .reduceByKey(add, numPartitions = 840) \
                .map(lambda (token, count): (count, token)) \
                .sortByKey(False, numPartitions = 840) \
                .take(1000)

    return closest.count()

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf8')
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "PredictUsers", pyFiles=['predictUsers.py'])

    dir_path = '/user/arapat/twitter-tag/'
    # files = [dir_path + 't01']
    files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    # files = [dir_path + 't%02d' % k for k in range(1, 10)]
    print predict_users(files)

