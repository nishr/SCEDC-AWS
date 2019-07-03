import sys
import os
sys.path.append(os.path.abspath("~/SCEDC-AWS/src/download_data/"))
from scedc_download import download
import datetime
starttime = datetime.datetime(2016,7,4)
endtime = datetime.datetime(2016,7,4)
network="CI"
channel = "HH*"
# minlongitude = -122.
# maxlongitude = -118.
# maxlatitude = 34.
# minlatitude = 32.
OUTDIR = "~/data/"
%timeit -n1 -r1 download(OUTDIR,starttime=starttime,endtime=endtime,network=network,channel=channel)
