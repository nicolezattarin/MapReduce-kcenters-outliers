"""
k-center with z outliers problem is the robust version of the k-center problem which is useful in 
the analysis of noisy data. 

Given a set P of points and two integers k and z, the problem requires to determine a set S ⊂ P of 
k centers which minimize the maximum distance of a point of P-Z from S, 
where Z are the z farthest points of P from S. 

In other words, with respect to the standard k-center problem, in the k-center with z 
outliers problem, we are allowed to disregard the z farthest points from S in the objective function. 

Unfortunately, the solution of this problem turns out much harder than the one of the standard k-center problem. 
The 3-approximation sequential algorithm by Charikar et al. for k-center with z outliers, 
which we call kcenterOUT, is simple to implement but has superlinear complexity 
(more than quadratic, unless sophisticated data structures are used). 

REPRESENTATION of POINTS: Euclidean space (real cooordinates) and with the Euclidean L2-distance. 

"""
import numpy as np
import sys
import os
import numpy as np

"""
Develop a method SeqWeightedOutliers(P,Weights,k,z,alpha) which implements the weighted variant of 
kcenterOUT (the 3-approximation algorithm for k-center with z-outliers). 

The method takes as input the set of points P, the set of weights Weights, the number of centers k, 
the number of outliers z, and the coefficient alpha used by the algorithm, a
nd returns the set of centers S computed as specified by the algorithm. 
guesses is understood that the i-th integer in Weights is the weight of the i-th point in P. 
Python users: represent P and S as list of tuple and Weights as list of integers. 
Considering the algorithm's high complexity, try to make the implementation as efficient as possible.  
"""
def euclidean(point1, point2):
    """
    Euclidean distance between two points.
    """
    import math
    res = 0
    for i in range(len(point1)):
        diff = (point1[i]-point2[i])
        res +=  diff*diff
    return math.sqrt(res)

#implement pair dist to check if its better

def ball(x, R, Set):
    """
    returns a list of points in the ball of radius R around x, the result is a subset of Set
    """
    mask = np.array([euclidean(p, x) for p in Set]) <= R # mask of the points in the ball

    from itertools import compress
    return list(compress(Set, mask))

def SeqWeightedOutliers(inputPoints, Weights, k, z, alpha, maxiter=1000, verbose=False):
    """
    returns the set of centers S computed as specified by the algorithm
    inputPoints (list of tuples): input points
    Weights (list of int): weights of the points
    """
    # min distance between first k+z+1 points and the centers
    from scipy.spatial import distance 
    pp = inputPoints[:k+z+1]

    global r # global variables to modify the values called in the main
    # DEBUG
    r = (min(np.extract(1-np.eye(len(pp)), distance.cdist(pp, pp)).flatten()) / 2)
    if verbose: print("r = ", r)

    global guesses 
    guesses = 1
    while guesses < maxiter:
        if verbose: print ('iteration:', guesses)

        import copy
        Z = copy.copy(inputPoints) # set of uncovered points, it is initialized with the entire set of points
        S = []                     # set of centers, it is initialized empty and it will be a list of tuples
        W_Z = np.sum(Weights)      # sum of weights of uncovered points, 
                                   # initialized as sum of the weights of the entire dataset

        while len(S) < k and W_Z > 0: # while we didnt fill the set of centers and there are uncovered points
            if verbose: print ('\nlen(S):', len(S))
            max = 0
            for x in inputPoints: # for each point in the set 
                # find the points in Z which are in a ball centered at x with radius (1+2*alpha)*r
                ball_points = ball(x, (1+2*alpha)*r, Z) #check this out (im assuming each point occurs only once)
                ball_weight = np.sum((Weights[inputPoints.index(b)] for b in ball_points))
                
                if verbose: print ('\nball_points:', ball_points, ' ball_weight:', ball_weight, ' max:', max, ' W_Z:', W_Z)

                # add to S the point ci ∈ inputPoints which maximizes the total weight in BZ(ci,(1+2α)r).
                if ball_weight > max: 
                    max = ball_weight
                    newcenter = x
                    
            # add the new center to the set of centers
            S.append(newcenter)
            if verbose: print ('S:', S)

            ball_points_newcenter = ball(newcenter, (3+4*alpha)*r, Z)
            if verbose: print ('ball_points_newcenter:', ball_points_newcenter)
            for y in ball_points_newcenter:
                Z.remove(y) # remove points from Z that have been assigned to the new center
                W_Z -= Weights[inputPoints.index(y)]
            if verbose:
                print ('Z:', Z, ' W_Z:', W_Z, '\n')

        if W_Z <= z: return S
        else: r*=2
        guesses += 1
    return S

def GetRGlobal():
    global r
    return r

def GetGuessesGlobal():
    global guesses
    return guesses

# Develop a method ComputeObjective(inputPoints,S,z) which computes the value of the objective function 
# for the set of points inputPoints, the set of centers S, and z outliers (the number of centers,
#  which is the size of S, is not needed as a parameter). Hint: you may compute all distances d(x,S), 
# for every x in inputPoints, sort them, exclude the z largest distances, and return the largest among the remaining ones. 
# Note that in this case we are not using weights!

def ComputeObjective(inputPoints, S, z):
    """
    returns the value of the objective function
    inputPoints list of tuples
    S list of tuples
    z int
    """
    dd = np.zeros(len(inputPoints)*len(inputPoints))
    for i in range(len(inputPoints)):
        for j in range(len(inputPoints)):
            dd[i+j] = euclidean(inputPoints[i], inputPoints[j])
    dd = np.sort(dd)
    return dd[-z]

    #try numpy version, check which is better?
    # from scipy.spatial import distance 
    # d = np.extract(1-np.eye(len(inputPoints)+1), distance.cdist(inputPoints, S)).flatten()
    # return np.partition(d, -z)[-z]
