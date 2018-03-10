__precompile__()

module RowTables

using Compat
import DataFrames
import DataFrames.rename!
import DataStructures.OrderedDict
using JSON

if VERSION >= v"0.7.0-DEV.2738"
    const kwpairs = pairs
    using Random  #  this is is not the correct version. have to look that up
else
    kwpairs(x::AbstractArray) = (first(v) => last(v) for v in x)
end
if VERSION >= v"0.7.0-DEV.2915"
    using Unicode
end
if VERSION >= v"0.7.0-DEV.3052"
    using Printf
end


export AbstractRowTable, RowTable, rows, columns, rename!, rowdict

include("index.jl")
include("abstractrowtable.jl")
include("rowtable.jl")
include("sort.jl")

end  # module RowTables
