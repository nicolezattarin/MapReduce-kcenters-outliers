import numpy as np
import time
import sys, os
import math
from scipy.spatial import distance
from kcenters import SeqWeightedOutliers, ComputeObjective

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

    # read data
    P = readVectorsSeq(path)
    weights = [1] * len(P)
       
    print("Input size = ", len(P))
    print("Number of centers k = ", k)
    print("Number of outliers z = ", z)

    # run algorithm
    t = time.time()
    S = SeqWeightedOutliers(P, weights, k, z, 0)
    t = (time.time() - t)

    #compute objective
    objective = ComputeObjective(P, S, z)
    print("Objective function = ", objective)
    print("Time of SeqWeightedOutliers = ", t*1000)
    

if __name__ == "__main__":
    main()
