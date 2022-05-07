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

"""
Develop a method SeqWeightedOutliers(P,W,k,z,alpha) which implements the weighted variant of 
kcenterOUT (the 3-approximation algorithm for k-center with z-outliers). 

The method takes as input the set of points P, the set of weights W, the number of centers k, 
the number of outliers z, and the coefficient alpha used by the algorithm, a
nd returns the set of centers S computed as specified by the algorithm. 
guesses is understood that the i-th integer in W is the weight of the i-th point in P. 
Python users: represent P and S as list of tuple and W as list of integers. 
Considering the algorithm's high complexity, try to make the implementation as efficient as possible.  
"""
def euclidean(point1,point2):
    import math
    res = 0
    for i in range(len(point1)):
        diff = (point1[i]-point2[i])
        res +=  diff*diff
    return math.sqrt(res)

#implement pair dist to check if its better

def ball(x, R, Set):
    """
    returns a list of points in the ball of radius r around x
    """
    mask = np.array([euclidean(p, x) for p in Set]) < R
    from itertools import compress
    return list(compress(Set, mask))

def SeqWeightedOutliers(P, W, k, z, alpha, maxiter=1000, verbose=True):
    """
    returns the set of centers S computed as specified by the algorithm
    P list of tuples
    W list of int
    """
    # min distance between first k+z+1 points and the centers
    from scipy.spatial import distance 
    pp = P[:k+z+1]

    global r, guesses # global variables to modify the values called in the main

    # DEBUG
    r = (min(np.extract(1-np.eye(len(pp)), distance.cdist(pp, pp)).flatten()) / 2)
    if verbose: print("r = ", r)

    guesses = 1
    while guesses < maxiter:
        if verbose: print ('iteration:', guesses)
        import copy
        Z = copy.copy(P)
        S = []
        W_Z = np.sum(W)
        while len(S) < k and W_Z > 0:
            if verbose: print ('\nlen(S):', len(S))
            max = 0
            for x in P:
                ball_points = ball(x, (1+2*alpha)*r, Z) #check this out (im assuming all elements are different)
                ball_weight = np.sum((W[P.index(b)] for b in ball_points))#check this out
                
                if verbose: print ('ball_points:', ball_points, 'ball_weight:', ball_weight, 'max:', max, 'W_Z:', W_Z)

                # add to S the point ci ∈ P which maximizes the total weight in BZ(ci,(1+2α)r).
                if ball_weight > max:
                    max = ball_weight
                    newcenter = x
                S.append(newcenter)
                ball_points_newcenter = ball(newcenter, (3+4*alpha)*r, Z)
                if verbose: print ('ball_points_newcenter:', ball_points_newcenter)
                for y in ball_points_newcenter:
                    Z.remove(y)
                    W_Z -= W[P.index(y)]
                    if verbose:
                        print ('y:', y)
                        print ('Z:', Z)
                        print ('W_Z:', W_Z)
        if W_Z <= z: return S
        else: r*=2
        guesses += 1
    return S


# Develop a method ComputeObjective(P,S,z) which computes the value of the objective function 
# for the set of points P, the set of centers S, and z outliers (the number of centers,
#  which is the size of S, is not needed as a parameter). Hint: you may compute all distances d(x,S), 
# for every x in P, sort them, exclude the z largest distances, and return the largest among the remaining ones. 
# Note that in this case we are not using weights!

def ComputeObjective(P, S, z):
    """
    returns the value of the objective function
    P list of tuples
    S list of tuples
    z int
    """
    # dd = np.zeros(len(inputPoints)*len(inputPoints))
    # for i in range(len(inputPoints)):
    #     for j in range(len(inputPoints)):
    #         dd[i+j] = euclidean(inputPoints[i], inputPoints[j])
    # d = np.sort(d)
    # return d[-z]

    #try numpy version, check which is better?
    from scipy.spatial import distance 
    d = np.extract(1-np.eye(len(P)), distance.cdist(P, S)).flatten()
    return np.partition(d, -z)[-z]
