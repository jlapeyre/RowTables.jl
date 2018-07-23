using RowTables
using DataFrames
using BenchmarkTools

## DataFrames will not run on Jupyter at the moment

ds = [
    Dict(:a=>2,:b=>1,:c=>7)
    Dict(:a=>1,:b=>4,:c=>8)
    Dict(:a=>9,:b=>6,:c=>4)
    Dict(:a=>10,:b=>8,:c=>7)
    Dict(:a=>10,:b=>4,:c=>3)];

rt = RowTable(ds,[:a,:b,:c])

df = DataFrame(rt)

df[3,:]

rt[3,:]

rt[3:3,:]

@btime rt[3,:]

@btime df[3,:]

@btime rt[3:3,:]

@btime rt[3:4,:]

@btime df[3:4,:]

@btime df[3,:]

df[:,1]

rt[:,1]

@btime rt[:,1];

@btime df[:,1];

@btime DataFrame(rt)

@btime RowTable(df)



