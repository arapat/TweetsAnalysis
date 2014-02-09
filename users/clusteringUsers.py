# Note that the add operation in K-means algorithm is defined WITHOUT factors

import os
import json
from operator import add
from pyspark import SparkContext


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

    def diff(dist1, dist2):
        vec1 = dist1[2]
        vec2 = dist2[2]
        len1 = len(vec1)
        len2 = len(vec2)
        result = sum([item[1] ** 2 for item in vec1] + [item[1] ** 2 for item in vec2])
        itr1 = 0
        itr2 = 0
        while itr1 < len1 and itr2 < len2:
            if vec1[itr1][0] < vec2[itr2][0]:
                itr1 += 1
            elif vec1[itr1][0] > vec2[itr2][0]:
                itr2 += 1
            else:
                result -= 2 * vec1[itr1][1] * vec2[itr2][1]
                itr1 += 1
                itr2 += 1
        return result


    # def union(dist1, dist2):
    #     vec1 = dist1[2]
    #     vec2 = dist2[2]
    #     len1 = len(vec1)
    #     len2 = len(vec2)
    #     result = []
    #     itr1 = 0
    #     itr2 = 0
    #     while itr1 < len1 and itr2 < len2:
    #         if vec1[itr1][0] < vec2[itr2][0]:
    #             result.append(vec1[itr1])
    #             itr1 += 1
    #         elif vec1[itr1][0] > vec2[itr2][0]:
    #             result.append(vec2[itr2])
    #             itr2 += 1
    #         else:
    #             result.append((vec1[itr1][0], vec1[itr1][1] + vec2[itr2][1]))
    #             itr1 += 1
    #             itr2 += 1
    #     while itr1 < len1:
    #         result.append(vec1[itr1])
    #         itr1 += 1
    #     while itr2 < len2:
    #         result.append(vec2[itr2])
    #         itr2 += 1
    #     return [0, 0, result]
    

    def average(dist, k):
        result = dist[2]
        for itr in range(len(result)):
            result[itr] = (result[itr][0], float(result[itr][1]) / k)
        return [dist[0], dist[1], result]


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
        return (uid, counter, vec)


    def normalize_center(center):
        uid = center[0]
        counter = float(center[1][0])
        vec = sorted(center[1][1])

        result = []
        tp = vec[0]
        for i in range(1,len(vec)):
            if tp[0] == vec[i][0]:
                tp = (tp[0], tp[1] + vec[i][1])
            else:
                vec.append(tp)
                tp = vec[i]
        vec.append(tp)
        vec = map(lambda (token, weight): (token, weight / counter), vec)
        return (uid, (uid, counter, vec))


    def closestPoint(p, centers):
        bestIndex = 0
        closest = float("+inf")
        for i in range(len(centers)):
            tempDist = diff(p, centers[i]) # np.sum((p - centers[i]) ** 2)
            if tempDist < closest:
                closest = tempDist
                bestIndex = i
        return bestIndex


    if len(files) == 0:
        return 0

    # Ignore users that have fewer than MIN_POST tweets
    MIN_POST = 7

    # For each account, put all the tokens of its tweets into one vector
    all_users = None
    for file_name in files:
        users = gen_rdd(file_name)
        if all_users:
            all_users = all_users.union(users)
        else:
            all_users = users

    # Normalize the vector
    all_users = all_users.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .filter(lambda (uid, (counter, tokens)): counter >= MIN_POST) \
            .map(normalize)

    # Clustering (k-means)
    K = 10
    CONVERGE = 0.3

    kPoints = all_users.takeSample(True, K, 1)
    temp_dist = 1.0

    while temp_dist > CONVERGE:
        closest = all_users.map(
            lambda p : (closestPoint(p, kPoints), (1, p[2])))
        newPoints = closest.reduceByKey(
            lambda (counter1, p1), (counter2, p2): (counter1 + counter2, p1 + p2)) \
                    .map(normalize_center).collect()

        tempDist = sum(diff(kPoints[cid], ctr) for (cid, ctr) in newPoints)

        for (x, y) in newPoints:
            kPoints[x] = y

    print "Final centers:"
    for point in kPoints:
        print point[2]
    return all_users.count()
 

if __name__ == "__main__":
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "ClusteringUsers", pyFiles=['clusteringUsers.py'])

    dir_path = '/user/arapat/twitter-tag/'
    files = [dir_path + 't01', dir_path + 't02']
    # files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    print clustering_user(files)

