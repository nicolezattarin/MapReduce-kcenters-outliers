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
    print("solution: ", solution)

    #initial guess for r
    P = inputPoints[:k+z+1]
    from scipy.spatial import distance 
    rInit = (min(np.extract(1-np.eye(len(P)), distance.cdist(P, P)).flatten()) / 2)

    # Run ComputeObjective(inputPoints,solution,z) and save the output in a variable called objective.
    from kcenters import ComputeObjective
    objective = ComputeObjective(inputPoints,solution, z)

    # output
    print("Input size n = {}".format(len(inputPoints)))
    print("Number of centers k = {}".format(k))
    print("Number of outliers z = {}".format(z))
    print("Initial guess = {}".format(rInit))
    print("Final guess = {}".format(GetRGlobal()))
    print("Number of guesses = {}".format(GetGuessesGlobal()))
    print("Objective function = {}".format(objective))
    print("Time of SeqWeightedOutliers = {}".format(alg_time))

    # plot 2dimensional points and centers
    import matplotlib.pyplot as plt
    import seaborn as sns
    if len(inputPoints[0]) == 2:
        sns.set_theme(style="white", font_scale=2)
        fig, ax = plt.subplots(figsize=(10,10))
        colorPoints = "darkorange"
        colorCenters = "teal"
        ms = 120
        sns.scatterplot(x=[i[0] for i in inputPoints], y=[i[1] for i in inputPoints], 
                        color=colorPoints, ax=ax, s=ms)
        sns.scatterplot(x=[i[0] for i in solution], y=[i[1] for i in solution], color=colorCenters, 
                        ax=ax, s=ms)

        # plot distances
        distances = np.zeros(shape=(len(inputPoints), len(solution))) #rows are points, columns are centers 
        for i in range(len(inputPoints)):
            for j in range(len(solution)):
                distances[i, j] = euclidean(inputPoints[i], solution[j])
                
        for i in range(len(inputPoints)):
            center = solution[np.argmin(distances[i, :])]
            # DEBUG
            # print("center of point {} is {}".format(inputPoints[i], center))
            sns.lineplot(x=[inputPoints[i][0], center[0]], y=[inputPoints[i][1], center[1]],
                            color='black', ax=ax, linewidth=1.5, linestyle='--', markersize=0)
        

        fig.savefig("kcenter_k"+str(k)+"_z"+str(z)+"_test.png", bbox_inches='tight')

if __name__ == "__main__":
    main()