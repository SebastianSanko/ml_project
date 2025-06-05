#!/bin/bash
cd /home/jovyan/workspace/ml_project && conda env create -f "environment.yml" && python -m nb_conda_kernels list