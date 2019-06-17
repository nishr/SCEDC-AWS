import boto3  
from boto3.dynamodb.conditions import Key, Attr 
from decimal import Decimal 
import datetime  
from math import ceil  
import os  

def download(OUTDIR,starttime=None, endtime=None,
                     network=None, station=None, location=None, channel=None,
                     minlatitude=None, maxlatitude=None, minlongitude=None,
                     maxlongitude=None):
    """
    Query the data from SCEDC on S3. 


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
    s3 = boto3.resource('s3')
    bucket = "scedc-pds"

    for ii in range(len(out_files)):
        s3.meta.client.download_file(bucket,files2download[ii],out_files[ii])
        print("Downloading file: {}, {} of {}".format(files2download[ii],
                ii+1,len(files2download)),end="\r")
    
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
