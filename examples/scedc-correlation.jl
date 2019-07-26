using Distributed
addprocs(3)

function aws_available(ROOT::String; format::String="ms")
    files = glob("*"*format,ROOT)
    N = length(files)
    NET = Array{String,1}(undef,N)
    STA = Array{String,1}(undef,N)
    LOC = Array{String,1}(undef,N)
    CHAN = Array{String,1}(undef,N)
    STARTTIME = Array{Date,1}(undef,N)

    for ii = 1:N
        path, file = splitdir(files[ii])
        NET[ii] = file[1:2]
        STA[ii] = strip(file[3:7],'_')
        CHAN[ii] = file[8:10]
        LOC[ii] = strip(file[11:13],'_')
        STARTTIME[ii] = yearday(file[14:17],file[18:20])
    end

    df = DataFrame(NET = NET, STA = STA, LOC = LOC, CHAN = CHAN,
               STARTTIME = STARTTIME, FILE = files)
    return df
end

function yearday(year,day)
       return Date(Year(year)) + Day(day) - Day(1)
end

@everywhere begin
    using Glob, Dates, SeisIO, SeisNoise, DataFrames

function fftpar(filename)
    S = read_data("mseed",filename)
    println("Computing FFT for $(S[1].name)")
    FFT = compute_fft(S,freqmin, freqmax, fs, cc_step, cc_len,
                      time_norm=time_norm,to_whiten=to_whiten)
    return FFT
end

cc_len = 1800
cc_step = 450
fs = 20.
freqmin = 0.05
freqmax = 1.
time_norm = false
to_whiten = false
maxlag = 200.
smoothing_half_win = 20
corr_type = "coherence"
ROOT = "/home/ubuntu/data/2016/2016_186/"
OUTDIR = "/home/ubunutu/CI/CORR/"
makepath(OUTDIR)
end

df = aws_available(ROOT) # get data available
df = df[df[:CHAN] .== "HHZ",:] # take just HHZ
ind = findall(nonunique(df,[:NET,:STA,:CHAN])) # find duplicated locations
deleterows!(df,ind) # delete non-unique net-sta-chans
# df = df[1:8:end,:]
files = df[:,:FILE] # get file names

FFTS = pmap(fftpar,files) # compute all FFTs in memory/parallel
# compute correlations in parallel
corrmap(reverse(FFTS),maxlag,smoothing_half_win,corr_type,OUTDIR)
