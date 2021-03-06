using Pkg

# add following packages
Pkg.add.(["AWSCore","AWSS3","DataFrames", "DSP", "FFTW", "Glob", "JLD2", 
       "Interpolations", "GLM", "Plots","CSV", "Glob","BenchmarkTools"])
Pkg.add(PackageSpec(name="SeisIO", rev="master"))
Pkg.add(PackageSpec(name="SeisNoise", rev="GPU"))
