# Note that the add operation in K-means algorithm is defined WITHOUT factors

import json
import os
import sys
from operator import add
from pyspark import SparkContext

import numpy as np
from scipy.sparse import csc_matrix
# from scipy.sparse import dok_matrix

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


def clustering_user(files):

    w_dict = None
    w_len = 0
    kPoints = None

    sqsum = lambda v: np.sum(v.data * v.data)

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
                # Do nothing: filter out words appears infrequently
                pass
            if idx:
                row.append(idx)
                data.append(vec[i][1])

        return (uid, csc_matrix((data, (row, [0] * len(row))), shape=(w_len, 1)))


    def closestPoint(user):
        vec = user[1]
        bestIndex = 0
        closest = float("+inf")
        for i in range(len(kPoints)):
            tempDist = sqsum(vec - kPoints[i])
            if tempDist < closest:
                closest = tempDist
                bestIndex = i
        # return (bestIndex, (vec, 1, user[0]))
        return (bestIndex, (vec, 1))


    if len(files) == 0:
        return 0

    # Ignore users that have fewer than MIN_POST tweets
    MIN_POST = 7
    MIN_OCCURS = 10000
    # MIN_POST = 0
    # MIN_OCCURS = 0

    # For each account, put all the tokens of its tweets into one vector
    all_users = None
    for file_name in files:
        users = gen_rdd(file_name)
        if all_users:
            all_users = all_users.union(users)
        else:
            all_users = users

    # Normalize the vector
    all_users = all_users.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]), numPartitions = 480) \
            .filter(lambda (uid, (counter, tokens)): counter >= MIN_POST)

    # 4606200 words
    all_words = all_users.flatMap(lambda (uid, (counter, tokens)): [(w, 1) for w in tokens]) \
            .reduceByKey(add, numPartitions = 240) \
            .filter(lambda (w, count): count >= MIN_OCCURS) \
            .map(lambda (w, count): w).collect()
    all_words = sorted(all_words)
    w_len = len(all_words)
    w_dict = {all_words[i]: i for i in range(len(all_words))}

    print w_len

    # Construct features
    all_users = all_users.map(normalize).map(to_sparse).cache()

    # Clustering (k-means)
    K = 30
    CONVERGE = 0.1
    MAXITER = 10
    temp_dist = 1.0
    kPoints = [vec for (uid, vec) in all_users.takeSample(False, K, 1)]

    iter = 0
    while temp_dist > CONVERGE and iter < MAXITER:
        closest = all_users.map(closestPoint)
        pointStats = closest.reduceByKey(
            lambda (vec1, counter1), (vec2, counter2): (vec1 + vec2, counter1 + counter2), \
            numPartitions = 480)
        newPoints = pointStats.map(
            lambda (cid, (vec, counter)): (cid, vec / counter)).collect()

        temp_dist = np.sum([sqsum(kPoints[cid] - vec) for (cid, vec) in newPoints])
        print "Iteration %d:" % (iter + 1), temp_dist

        for (cid, vec) in newPoints:
            kPoints[cid] = vec

        iter = iter + 1

    print "Final centers:"
    for point in kPoints:
        print "=========="
        row, col = point.nonzero()
        for i,j in zip(row, col):
            print i,j,point[i,j]
        print "=========="
    print "w_len =", w_len

    # print '\n\nSize of each group:'
    # closest = all_users.map(closestPoint).cache()
    # for i in range(30):
    #     print "Group %02d" % i
    #     print [t[1][2] for t in closest.filter(lambda (index, others): index == i).collect()]

    return all_users.count()
 

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf8')
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "ClusteringUsersPartitions", pyFiles=['clusteringUsersFixed.py'])

    dir_path = '/user/arapat/twitter-tag/'
    # files = ['/user/arapat/twitter-sample/tag100']
    # files = [dir_path + 't01']
    # files = [dir_path + 't%02d' % k for k in range(1, 11)]
    files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    print " ".join(["All users:", str(clustering_user(files))])

