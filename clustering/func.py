import numpy as np
from pyspark import SparkContext, SparkConf
import sys, os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def assign(set, centers):
    def distance(pair, centers):
        """
        return the pair (index of the point, index of the center)
        the index of of the center correspons with the assigned cluster
        """
        point_idx = pair[0]
        point = pair[1]
        distances = np.array([np.linalg.norm(point - center) for center in centers])
        center_idx = np.argmin(distances)
        return (point_idx, center_idx)
    assigned_set = set.map(lambda pair: distance(pair, centers))
    return assigned_set

def FarthestFirstTraversal (set):
    npoints = set.count()
    center0 = set.takeSample(False, 1)[0][1]
    
    
    # Show that the Farthest-First Traversal algorithm can 
    # be implemented to run in O (N · k) time.
    # Hint: make sure that in each iteration i of the forloop 
    # each point p∈P−S knowsitsclosestcenteramongc1,c2,...,ci−1 a
    # ndthe distance from such a center.