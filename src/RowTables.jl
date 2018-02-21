#__precompile__()

module RowTables

using Compat
import DataFrames
import DataFrames.rename!


if VERSION >= v"0.7.0-DEV.2738"
    const kwpairs = pairs
else
    kwpairs(x::AbstractArray) = (first(v) => last(v) for v in x)
end
if VERSION >= v"0.7.0-DEV.2915"
    using Unicode
end
if VERSION >= v"0.7.0-DEV.3052"
    using Printf
end


export AbstractRowTable, RowTable, rows, rename!

include("index.jl")
include("abstractrowtable.jl")
include("rowtable.jl")

end  # module RowTables
