#!/bin/bash
cd /home/jovyan/workspace/proj && conda env create -f "environment.yml" && python -m nb_conda_kernels list