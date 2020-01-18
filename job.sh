#!/bin/bash
# Below the directives for SGE
#$ -pe mpi 2
#$ -R y
mpirun python36 /shared/skeleton_mpi.py
