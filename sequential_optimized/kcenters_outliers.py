import numpy as np
import time
import sys, os
import math
from scipy.spatial import distance

def euclidean(point1,point2):
    """
    compute the euclidean distance between two points
    """
    res = 0
    for i in range(len(point1)):
        diff = (point1[i]-point2[i])
        res +=  diff*diff
    return math.sqrt(res)

def ComputeObjective(P,S,z):
    """
    Compute the objective function
    params:
        P (list): list of points
        S (list): list of centers
        z (int): number of outliers
    return:
        objective (float): objective function
    """
    P_clean = [x for x in P if x not in S]
    dist_from_S = []
    for x in P_clean:
        dist_from_S.append(min([euclidean(x, s) for s in S]))
    dist_from_S.sort()
    if z == 0: return max(dist_from_S)
    else: return max(dist_from_S[:-z])


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

    # distances = np.zeros((len(P), len(P)))
    # for i in range(len(P)):
    #     for j in range(len(P)):
    #         distances[i][j] = euclidean(P[i], P[j])
    #         distances[j][i] = distances[i][j] 
    
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