using Dates, AWSCore, AWSS3, DataFrames

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
                        maxradius::Union{Float64,Nothing}=nothing)

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
    filecsv = s3_get(aws,"scedc-test","download/file_list.csv");
    filedf = readtable(IOBuffer(filecsv))
    filedf[:DATE] = Date.(filedf[:DATE])
    println("Filtering query           $(now())")

    # filter by lat/lon
    if !isnothing(minlatitude)
        filedf = filedf[filedf[:LAT] .> minlatitude,:]
    end

    if !isnothing(maxlatitude)
        filedf = filedf[filedf[:LAT] .< maxlatitude,:]
    end

    if !isnothing(minlongitude)
        filedf = filedf[filedf[:LON] .> minlongitude,:]
    end

    if !isnothing(maxlongitude)
        filedf = filedf[filedf[:LON] .< maxlongitude,:]
    end

    # filter by time
    if !isnothing(starttime)
        filedf = filedf[filedf[:DATE] .>= starttime,:]
    end

    if !isnothing(endtime)
        filedf = filedf[filedf[:DATE] .<= endtime,:]
    end

    # filter stations
    df_subset!(filedf,network,:NET)
    df_subset!(filedf,station,:STA)
    df_subset!(filedf,channel,:CHAN)
    df_subset!(filedf,location,:LOC)

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
    Threads.@threads for ii = 1:length(files2download)
        s3_get_file(aws, "scedc-pds", files2download[ii], out_files[ii])
        print("Downloading file: $(files2download[ii])       \r")
    end
    println("Download Complete!        $(now())          ")
    tend = now()
    println("Download took $(Dates.canonicalize(Dates.CompoundPeriod(tend - tstart)))")
end

function df_subset!(df::DataFrame,col::String,colsymbol::Symbol)
        col = regex_helper(col)
        ind = occursin.(col,df[colsymbol])
        deleterows!(df,.!ind)
end

function df_subset!(df::DataFrame,col::Nothing,colsymbol::Symbol)
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
