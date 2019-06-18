import os 
import datetime
from math import ceil   
from concurrent import futures
import boto3  
from boto3.dynamodb.conditions import Key, Attr 
from decimal import Decimal 


def download(OUTDIR,starttime=None, endtime=None,
                     network=None, station=None, location=None, channel=None,
                     minlatitude=None, maxlatitude=None, minlongitude=None,
                     maxlongitude=None):
    """
    Download SCEDC data from S3. 

    Args:
        OUTDIR (str): The output directory.
        starttime (datetime.datetime): The starttime of the download. 
        endtime (datetime.datetime): The endtime of the download. 
        network (str): Network to download from. If network = "*" or is unspecified, 
                       data is downloaded from all available networks. 
        station (str): Station to download, e.g. "RFO". If station = "*" or is unspecified, 
                       data is downloaded from all available stations. 
        channel (str): Channels to download, e.g. "HH*". If channel = "*" or is unspecified, 
                       data is downloaded from all available channels.
        channel (str): Locations to download, e.g. "00". If channel = "*" or is unspecified, 
                       data is downloaded from all available locations. NOTE: most files do 
                       not have a location. 
        minlatitude (float): Minimum latitude in data search.
        maxlatitude (float): Maximum latitude in data search.
        minlongitude (float): Minimum longitude in data search.
        maxlongitude (float): Maximum longitude in data search.
    """

    # get request details and filter out Nones 
    locs = locals()
    locs = {k:v for k,v in locs.items() if v is not None}
    locs.pop('OUTDIR', None)
    locs = {k:datetime_to_str(v) for k,v in locs.items()}
    locs = {k:float_to_decimal(v) for k,v in locs.items()}
    request_keys = list(locs)
    map_req = {"starttime":"STARTTIME", "endtime":"ENDTIME", "network":"NET", 
                "station":"STA", "location":"LOC", "channel":"CHAN", 
                "minlatitude":"LAT", "maxlatitude":"LAT", "minlongitude":"LON",
                "maxlongitude":"LON"}

    # get number of concurrent threads for transfer 
    NUM_CPU = os.cpu_count()
    maxworkers = NUM_CPU * 10

    # create request filter
    FilterExpression = []
    for req,val in locs.items():
        if val == "*" or val == "":
            pass # no need to use wildcard on scans
        elif req in ["minlatitude", "minlongitude"]:
            FilterExpression.append('Attr("{}").gte({})'.format(map_req[req],val))
        elif req in ["maxlatitude", "maxlongitude"]:
            FilterExpression.append('Attr("{}").lte({})'.format(map_req[req],val))
        elif '*' in val:
            FilterExpression.append('Attr("{}").begins_with("{}")'.format(map_req[req],val.split("*")[0]))
        elif 'time' not in req:
            FilterExpression.append('Attr("{}").eq("{}")'.format(map_req[req],val))
    FilterExpression = ' & '.join(FilterExpression)

    # filter time expression 
    TimeExpression = []
    TimeExpression.append('(Attr("STARTTIME").between("{}","{}")'.format(locs["starttime"],locs["endtime"]))
    TimeExpression.append('(Attr("STARTTIME").lte("{}") & Attr("ENDTIME").gte("{}"))'.format(locs["starttime"],locs["endtime"]))
    TimeExpression.append('Attr("ENDTIME").between("{}","{}"))'.format(locs["starttime"],locs["endtime"]))
    TimeExpression = ' | '.join(TimeExpression)
    FilterExpression = ' & '.join([FilterExpression,TimeExpression])

    # set-up boto3 connections to dynamodb and s3 
    dynamodb = boto3.resource('dynamodb',region_name="us-west-2")
    table = dynamodb.Table('SCEDC-stations')

    # get results of query 
    response = table.scan(FilterExpression=eval(FilterExpression),ProjectionExpression="stationID")
    if not response["Items"]:
        print("No results for query: {}".format(FilterExpression))
        return 

    # query stations from SCEDC-files
    table = dynamodb.Table('SCEDC-files')
    stations = [d["stationID"] for d in response["Items"]]
    stations = sorted(stations)

    # query files to download 
    files2download = []
    for station in stations:
        KeyConditionExpression = []
        filter_str = ', '.join(["\'{}\'".format(s) for s in stations])
        KeyConditionExpression.append('Key("stationID").eq("{}")'.format(station))
        KeyConditionExpression.append('Key("DATE").between("{}","{}")'.format(
                                locs["starttime"],locs["endtime"]))
        KeyConditionExpression = ' & '.join(KeyConditionExpression)
        fileresponse = table.query(KeyConditionExpression=eval(KeyConditionExpression),
                                ProjectionExpression="FILEPATH")

        if fileresponse["Items"]:
            files2download.append(fileresponse["Items"])
        while 'LastEvaluatedKey' in fileresponse:
            response = table.query(
            ProjectionExpression="FILEPATH",
            KeyConditionExpression=eval(KeyConditionExpression),
            ExclusiveStartKey=fileresponse['LastEvaluatedKey'])
            if fileresponse["Items"]:
                files2download.append(fileresponse["Items"])

    # get all files to dowload in one list 
    files2download = [file["FILEPATH"] for stat in files2download for file in stat]
    if "~" in OUTDIR:
        OUTDIR = os.path.expanduser(OUTDIR)
    out_files = [os.path.join(OUTDIR,f) for f in files2download]
    file_dir = list(set([os.path.dirname(f) for f in out_files]))

    # create directories if necessary 
    for direc in file_dir:
        if not os.path.isdir(direc):
            os.makedirs(direc)

    # download files from s3 to ec2
    s3 = boto3.client('s3')
    bucket = "scedc-pds"

    # helper functions within the local scope
    def fetch(key):
        out = os.path.join(OUTDIR,key)
        s3.download_file(bucket, key, out)
        print("Downloading file: {}".format(key),end="\r")
        return 

    def fetch_all(keys,maxworkers=5):
        print("Starting Download...\n")
        futureL = []
        with futures.ThreadPoolExecutor(max_workers=maxworkers) as executor:
            for ii in range(len(keys)):
                future = executor.submit(fetch,keys[ii])
                futureL.append(future)
        futures.wait(futureL)
        return 

    # download all the data in parallel
    fetch_all(files2download,maxworkers=maxworkers)
    
    return 

def datetime_to_str(value):
    if type(value) == datetime.datetime:
        return datetime.datetime.strftime(value,"%Y-%m-%d")
    else: 
        return value

def float_to_decimal(value):
    if type(value) == float:
        return Decimal(value)
    else: 
        return value

