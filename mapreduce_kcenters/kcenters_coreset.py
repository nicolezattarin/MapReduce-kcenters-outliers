import numpy as np
from scipy.spatial import distance
import random
import time
import math

# To avoid: Exception: Python in worker has different version than that in driver , 
# PySpark cannot run with different minor versions.
# Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.
import os,sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def MR_kCenterOutliers(points, k, z, L):
    """
    run MR k-center with outliers algorithm
    params:
        points: RDD of points
        k: number of centers
        z: number of outliers
        L: number of partitions
    return:
        centers: a list of centers

    In Round 1 it extracts k+z+1 coreset points from each partition using method kCenterFFT
    which implements the Farthest-First Traversal algorithm, and compute the weights of the
    coreset points using method computeWeights. 
    
    In Round 2, it collects the weighted coreset into a local data structure and runs kcenters
    to extract and return the final set of centers.
    """
    
    #------------- ROUND 1 ---------------------------
    # extract coreset from each partition
    # note that if the number of points per partition is too large wrt thenumber of points, 
    # the algorithm could fail because some coreset end up being empty 
    t = time.time()
    coreset = points.mapPartitions(lambda iter: extractCoreset(iter, k, z))
    print("Round 1: ", (time.time() - t)*1000, " ms")
    
    #------------- ROUND 2 ---------------------------
    # get weights and points for each coreset
    elems = coreset.collect()
    coresetPoints = list() 
    coresetWeights = list()
    for i in elems:
        coresetPoints.append(i[0])
        coresetWeights.append(i[1])
    # run kcenters
    t = time.time()
    centers = SeqWeightedOutliers(coresetPoints, coresetWeights, k, z, alpha=2)
    print("Round 2: ", (time.time() - t)*1000, " ms")
    
    return centers

def SeqWeightedOutliers(P, W, k, z, alpha):
    """
    Sequential Weighted Outliers Algorithm
    params:
        P (list): list of points
        W (list): list of weights
        k (int): number of centers
        z (int): number of outliers
        alpha (float): parameter for the ball computation
    return:
        S (list of tuples): list of centers

    Note that we previously compute all distances between all possible pairs of points in P.
    computing the distance matrix before the actual algorithm is 
    a matter of trade off between time and space, we choose to sacrifice n^2 memory in order 
    to avoid to recompute the distances each time 

    To compute the distance matrix a fast approach consists of using scipy: 
    ````
    from scipy.spatial import distance
    distances = distance.cdist(P, P, 'euclidean')
    ```
    we are not sure that scipy is installed on the machine, thus we also provide a second option, 
    based on numpy, which is slower, but safer in terms of modules dependencies.
    """
    distances = distance.cdist(P, P, 'euclidean')

    # compute min distance between first k+z+1 points of P    
    r = min(np.extract(1-np.eye(k+z+1),distances[:k+z+1,:k+z+1]).flatten()) / 2
    print("Initial guess = ", r)

    PP = np.array(P)
    WW = np.array(W)
    totalWeights = np.sum(W)

    niter, max_iter = 1, 100
    while (niter<max_iter):
        WZ = totalWeights
        centers = []
        # Z never changes size, it always has length equal to the number of points, 
        # but true values correspond to points which haven't been assigned to a cluster yet

        Z = np.ones(len(PP), dtype=bool) 
        while len(centers)<k and WZ>0:
            max = 0
            for i in range(len(PP)):
                # distances[i,:] is the distance between point i and all other 
                # points in P, weuse and to filter point in Z
                small_ball = np.logical_and(Z, distances[i,:] <= (1+2*alpha)*r)

                # ball is a mask of points in Z and in the ball of point i, True if the point is in the ball
                small_ball_weights = sum(WW[small_ball])

                # print("small_ball_weights = ", small_ball_weights)
                if small_ball_weights > max:
                    max = small_ball_weights
                    new_center = PP[i]
                    index_new_center = i

            # add new center to the list of centers
            centers.append(tuple(new_center))
            big_ball = np.logical_and(Z, distances[index_new_center,:] <= (3+4*alpha)*r)
            Z[big_ball] = False
            WZ -= sum(WW[big_ball])

        if WZ <= z: 
            print("Final guess = ", r)
            print("Number of guesses = ", niter)
            return list(centers)
        else: 
            niter+=1
            r*=2

def extractCoreset(iter, k, z):
    """
    extract a coreset from a given iterator
    params:
        iter: iterator
    """
    partition = list(iter)
    centers = kCenterFFT(partition, k+z+1)
    weights = computeWeights(partition, centers)
    c_w = list()
    for i in range(0, len(centers)):
        entry = (centers[i], weights[i])
        c_w.append(entry)
    # return weighted coreset
    return c_w

    
def kCenterFFT(points, k):
    """
    Method kCenterFFT: Farthest-First Traversal
    """
    idx_rnd = random.randint(0, len(points)-1)
    centers = [points[idx_rnd]]
    related_center_idx = [idx_rnd for i in range(len(points))]
    dist_near_center = [squaredEuclidean(points[i], centers[0]) for i in range(len(points))]

    for i in range(k-1):
        new_center_idx = max(enumerate(dist_near_center), key=lambda x: x[1])[0] # argmax operation
        centers.append(points[new_center_idx])
        for j in range(len(points)):
            if j != new_center_idx:
                dist = squaredEuclidean(points[j], centers[-1])
                if dist < dist_near_center[j]:
                    dist_near_center[j] = dist
                    related_center_idx[j] = new_center_idx
            else:
                dist_near_center[j] = 0
                related_center_idx[j] = new_center_idx
    return centers

def computeWeights(points, centers):
    """
    commpute weights of coreset
    params:
        points: list of points
        centers: list of centers
    """
    weights = np.zeros(len(centers))
    for point in points:
        mycenter = 0
        mindist = squaredEuclidean(point,centers[0])
        for i in range(1, len(centers)):
            dist = squaredEuclidean(point,centers[i])
            if dist < mindist:
                mindist = dist
                mycenter = i
        weights[mycenter] = weights[mycenter] + 1
    return weights

def strToVector(str):
    """
    convert a string to a vector
    """
    out = tuple(map(float, str.split(',')))
    return out

def squaredEuclidean(point1,point2):
    """
    compute squared euclidean distance between two points
    params:
        point1: first point
        point2: second point
    """
    res = 0
    for i in range(len(point1)):
        diff = (point1[i]-point2[i])
        res +=  diff*diff
    return res

def euclidean(point1,point2):
    """
    compute squared euclidean distance between two points
    params:
        point1: first point
        point2: second point
    """
    res = 0
    for i in range(len(point1)):
        diff = (point1[i]-point2[i])
        res +=  diff*diff
    return math.sqrt(res)

def ComputeObjective(points, centers, outliers):
    """
    Compute the objective function
    params:
        points (RDD): points
        centers (list): list of centers
        outliers (int): number of outliers
    return:
        objective (float): objective function
    """
    # compute the objective function with a strategy that tries to optimize the memory usage

    points = points.collect()
    P_clean = [x for x in points if x not in centers]
    dist_from_S = []

    for x in P_clean:
        dist_from_S.append(min([euclidean(x, s) for s in centers]))
    dist_from_S.sort()
    if outliers == 0: return max(dist_from_S)
    else: return max(dist_from_S[:-outliers])