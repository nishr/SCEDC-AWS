#!/bin/bash
cd
wget https://julialang-s3.julialang.org/bin/linux/x64/1.1/julia-1.1.1-linux-x86_64.tar.gz
tar xvfa julia-1.1.1-linux-x86_64.tar.gz
rm julia-1.1.1-linux-x86_64.tar.gz
echo PATH=\$PATH:~/julia-1.1.1/bin/ >> ~/.bashrc
source ~/.bashrc
julia ~/SCEDC-AWS/src/build_environment/julia/add-packages.jl
