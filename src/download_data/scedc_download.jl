export download
using Distributed
addprocs(length(Sys.cpu_info()) - 1)
@everywhere begin
using Dates, AWSCore, AWSS3, DataFrames

function df_subset!(df::DataFrame,col::String,colsymbol::Symbol)
        col = regex_helper(col)
        ind = occursin.(col,df[colsymbol])
        deleterows!(df,.!ind)
end

function df_subset!(df::DataFrame,col::Nothing,colsymbol::Symbol)
end

function df_remove!(df::DataFrame,col::String,colsymbol::Symbol)
        ind = occursin.(col,df[colsymbol])
        deleterows!(df,ind)
end

function regex_helper(reg::String)
    if reg == '*'
        # pass for all
    elseif occursin('?',reg)
        reg = replace(reg, '?' => '.')
        reg = Regex(reg)
    elseif occursin('*',reg)
        if reg[end] == '*'
                reg = '^' * strip(reg,'*')
        elseif reg[1] == '*'
                reg = strip(reg,'*') * '$'
        end
            reg = Regex(reg)
    end
    return reg
end

function s3_file_map(filein::String,fileout::String)
    s3_get_file(aws, "scedc-pds", filein, fileout)
    println("Downloading file: $filein       \r")
end

function filepath2stationID(df::DataFrame,starttime::Nothing,endtime::Nothing)
    N = size(df,1)
    stationID = Array{String}(undef,N)
    filepath = Array{String}(undef,N)
    date = Array{Date}(undef,N)
    for ii = 1:N
        file = df[ii,:FILEPATH]
        # get net.sta.loc.station
        net = file[1:2]
        sta = replace(file[3:7],"_"=>"")
        chan = file[8:10]
        loc = replace(file[11:13],"_"=>"")
        stationID[ii] = join([net,sta,loc,chan],'.')

        # update filepath
        y = file[14:17]
        d = file[18:20]
        filepath[ii] = y * '/' * (y * "_" * d) * '/' * file

        # add date
        date[ii] = yearday2Date(y,d)
    end

    filedf[:stationID] = stationID
    filedf[:FILEPATH] .= filepath
    filedf[:DATE] = date
    return filedf
end

function filepath2stationID(df::DataFrame,starttime::Date,endtime::Date)
    N = size(df,1)
    stationID = Array{String}(undef,0)
    filepath = Array{String}(undef,0)
    date = Array{Date}(undef,0)

    # convert starttime, endtime to string
    s = Date2yearday(starttime)
    e = Date2yearday(endtime)
    for ii = 1:N
        file = df[ii,:FILEPATH]

        # throw out dates
        if !(file[14:20] >= s && file[14:20] <= e)
            continue
        end
        # get net.sta.loc.station
        net = file[1:2]
        sta = replace(file[3:7],"_"=>"")
        chan = file[8:10]
        loc = replace(file[11:13],"_"=>"")
        push!(stationID,join([net,sta,loc,chan],'.'))

        # update filepath
        y = file[14:17]
        d = file[18:20]
        push!(filepath,y * '/' * (y * "_" * d) * '/' * file)

        # add date
        push!(date,yearday2Date(y,d))
    end
    newdf = DataFrame()
    newdf[:FILEPATH] = filepath
    newdf[:stationID] = stationID
    newdf[:DATE] = date
    return newdf
end

"""

  yearday2Date(year,day)

Convert year and day of year date format to `Date` object.
"""
function yearday2Date(year::String,day::String)
    day = parse(Int,day)
    return Date(year) + Day(day -1)
end

"""

  Date2yearday(year,day)

Convert `Date` object to yearday string, e.g. 2017354.
"""
function Date2yearday(d::Date)
    days = (d - Date(Year(d))).value + 1
    n = ndigits(days)
    return string(Year(d).value) * ('0' ^ (3 - n)) * string(days)
end

end

"""

  download()

Download SCEDC data from S3.

# Arguments
- `OUTDIR::String`: The output directory.
- `starttime::Date`: The starttime of the download.
- `endtime::Date`: The endtime of the download.
- `network::String`: Network to download from. If network = "*" or is unspecified,
                       data is downloaded from all available networks.
- `station::String`: Station to download, e.g. "RFO". If station = "*" or is unspecified,
                       data is downloaded from all available stations.
- `channel::String`: Channels to download, e.g. "HH*". If channel = "*" or is unspecified,
                       data is downloaded from all available channels.
- `location::String`: Locations to download, e.g. "00". If channel = "*" or is unspecified,
                       data is downloaded from all available locations. NOTE: most files do
                       not have a location.
- `minlatitude::Float64`: Minimum latitude in data search.
- `maxlatitude::Float64`: Maximum latitude in data search.
- `minlongitude::Float64`: Minimum longitude in data search.
- `maxlongitude::Float64`: Maximum longitude in data search.
"""

function download(OUTDIR::String;
                  starttime::Union{Date,Nothing}=nothing,
                  endtime::Union{Date,Nothing}=nothing,
                  network::Union{String,Nothing}=nothing,
                  station::Union{String,Nothing}=nothing,
                  location::Union{String,Nothing}=nothing,
                  channel::Union{String,Nothing}=nothing,
                  minlatitude::Union{Float64,Nothing}=nothing,
                  maxlatitude::Union{Float64,Nothing}=nothing,
                  minlongitude::Union{Float64,Nothing}=nothing,
                  maxlongitude::Union{Float64,Nothing}=nothing,
                  latitude::Union{Float64,Nothing}=nothing,
                  longitude::Union{Float64,Nothing}=nothing,
                  minradius::Union{Float64,Nothing}=nothing,
                  maxradius::Union{Float64,Nothing}=nothing,
                  stationXML::Bool=true)

    # check for inputs
    if all(.!isnothing.([starttime,endtime,network,station,location,channel,
                         minlatitude,maxlatitude,minlongitude,maxlongitude,
                         latitude,longitude,minradius,maxradius]))
        println("No inputs specified! Aborting download.")
        return
    end

    tstart = now()
    # connect to S3
    println("Connecting to AWS...      $(now())")
    aws = aws_config(region = "us-west-2")
    println("Downloading station file  $(now())")
    stationcsv = s3_get(aws,"scedc-test","download/scedc_station_list.csv");
    stationdf = readtable(IOBuffer(stationcsv))
    println("Filtering query           $(now())")

    # filter by lat/lon
    if !isnothing(minlatitude)
        stationdf = stationdf[stationdf[:LAT] .> minlatitude,:]
    end

    if !isnothing(maxlatitude)
        stationdf = stationdf[stationdf[:LAT] .< maxlatitude,:]
    end

    if !isnothing(minlongitude)
        stationdf = stationdf[stationdf[:LON] .> minlongitude,:]
    end

    if !isnothing(maxlongitude)
        stationdf = stationdf[stationdf[:LON] .< maxlongitude,:]
    end

    # filter stations
    df_subset!(stationdf,network,:NET)
    df_subset!(stationdf,station,:STA)
    df_subset!(stationdf,channel,:CHAN)
    df_subset!(stationdf,location,:LOC)

    # get file list
    filetxt = s3_get(aws,"scedc-test","download/scedc_file_list.txt");
    filedf = readtable(IOBuffer(filetxt), header=false,names=[:FILEPATH])
    filedf = filepath2stationID(filedf,starttime,endtime)

    # # filter by time
    # if !isnothing(starttime)
    #     filedf = filedf[filedf[:DATE] .>= starttime,:]
    # end
    #
    # if !isnothing(endtime)
    #     filedf = filedf[filedf[:DATE] .<= endtime,:]
    # end

    # remove unwanted stations
    stationID = intersect(stationdf[:,:stationID],filedf[:,:stationID])
    # return if nothing in dataframe
    if size(filedf,1) == 0
        println("No data available for request! Exiting.")
        return
    end
    filedf = filedf[∈(stationID).(filedf.stationID), :]

    # create directory for instrument responses
    if stationXML
        println("Downloading stationXML... $(now())")
        XMLDIR = joinpath(OUTDIR,"FDSNstationXML")
        mkpath(XMLDIR)
        networks = stationdf[:NET]
        stations = stationdf[:STA]
        xmlfiles = networks .* "_" .* stations .* ".xml"
        ind = indexin(unique(xmlfiles), xmlfiles)
        networks, stations, xmlfiles = networks[ind], stations[ind], xmlfiles[ind]
        xml_in = [joinpath("FDSNstationXML",networks[ii],xmlfiles[ii]) for ii = 1:length(xmlfiles)]
        xml_out = [joinpath(XMLDIR,networks[ii],xmlfiles[ii]) for ii = 1:length(xmlfiles)]
        xml_dir = unique([dirname(f) for f in xml_out])
        for ii = 1:length(xml_dir)
            if !isdir(xml_dir[ii])
                mkpath(xml_dir[ii])
            end
        end


        # check if requested channels have an instrument response
        stations2remove = []
        for ii = 1:length(xml_in)
            if s3_exists(aws,"scedc-pds",xml_in[ii])
                s3_get_file(aws,"scedc-pds",xml_in[ii],xml_out[ii])
            else
                push!(stations2remove,xmlfiles[ii])
            end
        end

        stations2remove = [replace(s[1:end-4],"_"=>".") for s in stations2remove]
        for ii = 1:length(stations2remove)
            df_remove!(filedf,stations2remove[ii],:stationID)
        end
    end

    # return if nothing in dataframe
    if size(filedf,1) == 0
        println("No data available for request! Exiting.")
        return
    end

    # query files
    files2download = filedf[:FILEPATH]

    # create directory structure
    println("Creating directories      $(now())")
    OUTDIR = expanduser(OUTDIR)
    out_files = [joinpath(OUTDIR,f) for f in files2download]
    file_dir = unique([dirname(f) for f in out_files])
    for ii = 1:length(file_dir)
        if !isdir(file_dir[ii])
            mkpath(file_dir[ii])
        end
    end

    # download files
    println("Starting Download...      $(now())")
    println("Using $(nworkers()) cores...")
    # Threads.@threads for ii = 1:length(files2download)
    #     s3_get_file(aws, "scedc-pds", files2download[ii], out_files[ii])
    #     print("Downloading file: $(files2download[ii])       \r")
    # end
    @eval @everywhere aws=$aws
    @eval @everywhere files2download=$files2download
    @eval @everywhere out_files=$out_files
    pmap(s3_file_map,files2download,out_files)
    println("Download Complete!        $(now())          ")
    tend = now()
    println("Download took $(Dates.canonicalize(Dates.CompoundPeriod(tend - tstart)))")
end
