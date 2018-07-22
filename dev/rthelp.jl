using Revise
using RowTables
using DataFrames
using DataFramesUtils
using BenchmarkTools

# Same example as in runtests
ds = [
    Dict(:a=>2,:b=>1,:c=>7)
    Dict(:a=>10,:b=>4,:c=>7)
    Dict(:a=>1,:b=>4,:c=>8)
    Dict(:a=>10,:b=>4,:c=>1)
    Dict(:a=>9,:b=>6,:c=>4)
    Dict(:a=>10,:b=>4,:c=>3)]

rt = RowTable(ds,[:a,:b,:c])
df = DataFrame(rt)

df2 = DataFramesUtils.gentestdf()
rt2 = RowTable(df2)

rt3 = RowTable([[rand(),rand(),rand()] for i in 1:10^3], [:a,:b,:c])

function mkbigdf(nr,nc)
    cols = Any[]
    for i in 1:nc
        push!(cols,rand(nr))
    end
    syms = [Symbol("x",i) for i in 1:nc]
    DataFrame(cols,syms)
end

function randsort(obj,ntrials)
    for i in 1:ntrials
        sort!(obj,cols=[rand(1:size(obj,2))])
    end
    nothing
end

function mkobjs()
    df = DataFrame(A = [45], B = ["F"])
    rt = RowTable(df)
    return (df,rt)
end

function appendrows(obj,n)
    nobj = copy(obj)
    for i in 1:n
        push!(nobj,[32, "F"])
    end
    nobj
end


nothing
