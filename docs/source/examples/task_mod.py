import numpy
from numpy.linalg import norm


def task(n):
    """Generates a 1xN array and computes its 2-norm"""
    A = numpy.ones(n)
    return norm(A, 2)
