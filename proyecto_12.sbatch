#!/bin/bash
#
#SBATCH --partition=all
#SBATCH --job-name=proyecto_12
#SBATCH --time=15:00
#SBATCH --output=output_12.%j.out
#SBATCH --ntasks=12
#

module purge
module load prun gnu7/7.3.0 openmpi3/3.1.0 py3-mpi4py/3.0.0

prun python proyectoFinalV10.py

