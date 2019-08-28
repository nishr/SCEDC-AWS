#!/bin/bash
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
if [ -d "/shared" ]
then
    bash miniconda.sh -b -p /shared/miniconda
echo 'export PATH="/shared/miniconda/bin:$PATH"' >> ~/.bashrc
else
    bash miniconda.sh -b -p $HOME/miniconda
    echo 'export PATH="$HOME/miniconda/bin:$PATH"' >> ~/.bashrc
fi
rm miniconda.sh
source ~/.bashrc

# create environment
conda env create -f ~/SCEDC-AWS/src/build_environment/python/py3.yml
conda clean -tipsy # clean up cache. Saves ~600 MB space
conda init bash
source ~/.bashrc
conda activate py3

