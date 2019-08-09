using Dates
include("/home/ubuntu/SCEDC-AWS/src/download_data/scedc_download.jl")

# This will download data all data in the scedc-pds bucket on 2017-07-01
# for all stations in the CI network with channels that begin with "H",
# e.g. "HHZ" or "HNE", in the specified lat/lon bounds.
# Files will be downloaded to the ~/data directory. 

starttime = Date("2016-07-01")
endtime = Date("2016-07-01")
network = "CI"
channel = "H??"
minlatitude = 32.
maxlatitude = 37.
minlongitude = -122.
maxlongitude = -117.
OUTDIR = "~/data"


download(OUTDIR, starttime=starttime, endtime=endtime, network=network,
         channel=channel, minlatitude=minlatitude,maxlatitude=maxlatitude,
         minlongitude=minlongitude,maxlongitude=maxlongitude)
