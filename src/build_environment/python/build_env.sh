#!/bin/bash
conda env create -f ./py3.yml
conda clean -tipsy # clean up cache. Saves ~600 MB space
conda init bash
. ~/.bashrc
