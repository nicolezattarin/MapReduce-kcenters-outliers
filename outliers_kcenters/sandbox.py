import numpy as np
import sys
import os
from kcenters import SeqWeightedOutliers
from kcenters import euclidean, GetRGlobal, GetGuessesGlobal
"""
command-line (CLI) arguments: 
    A path to a text file containing point set in Euclidean space. 
        Each line of the file contains, separated by commas, the coordinates of a point. 
        no assumptions on the number of dimensions
    An integer k (the number of centers).
    An integer z (the number of allowed outliers).
"""

def main ():
    def readVectorsSeq(filename):
        with open(filename) as f:
            result = [tuple(map(float, i.split(','))) for i in f]
        return result

    # command line arguments
    if len(sys.argv) != 4:
        print("Usage: python3", sys.argv[0], "<path_to_file> <k> <z>")
        sys.exit(1)
    path = sys.argv[1]
    k = sys.argv[2]
    z = sys.argv[3]
    if not k.isdigit(): raise ValueError("k must be an integer")
    if not z.isdigit(): raise ValueError("z must be an integer")
    if not os.path.isfile(path): raise ValueError("file not found")
    k = int(k)
    z = int(z)

    # read points
    inputPoints = readVectorsSeq(path) # list of tuples

    # DEBUG
    # print("inputPoints: ",inputPoints)
    # dd = np.zeros(len(inputPoints)*len(inputPoints))
    # for i in range(len(inputPoints)):
    #     for j in range(len(inputPoints)):
    #         dd[i+j] = euclidean(inputPoints[i], inputPoints[j])
    # from scipy.spatial import distance 
    # d = distance.cdist(inputPoints, inputPoints)
    # print("SCIPY: min distance:", np.min(d.flatten()), "max distance:", np.max(d.flatten()))
    # print("MANUAL: min distance:", np.min(dd), "max distance:", np.max(dd))
    # END DEBUG

    #Create list of integers called weights of the same cardinality of inputPoints, initialized with all 1's. 
    weights = [1] * len(inputPoints)
    
    # Run SeqWeightedOutliers(inputPoints,weights,k,z,0) to compute a set of (at most) k centers. 
    # The output of the method must be saved into a list of tuple called solution.
    import time
    t0 = time.time()
    solution = SeqWeightedOutliers(inputPoints,weights,k,z, 0)
    alg_time = time.time() - t0

    # DEBUG
    # print("solution: ", solution)

    #initial guess for r
    P = inputPoints[:k+z+1]
    from scipy.spatial import distance 
    rInit = (min(np.extract(1-np.eye(len(P)), distance.cdist(P, P)).flatten()) / 2)

    # Run ComputeObjective(inputPoints,solution,z) and save the output in a variable called objective.
    from kcenters import ComputeObjective
    objective = ComputeObjective(inputPoints,solution,z)

    # output
    print("Input size n = {}".format(len(inputPoints)))
    print("Number of centers k = {}".format(k))
    print("Number of outliers z = {}".format(z))
    print("Initial guess = {}".format(rInit))
    print("Final guess = {}".format(GetRGlobal()))
    print("Number of guesses = {}".format(GetGuessesGlobal()))
    print("Objective function = {}".format(objective))
    print("Time of SeqWeightedOutliers = {}".format(alg_time))

if __name__ == "__main__":
    main()