__precompile__()

module RowTables

using Compat
import DataFrames

export AbstractRowTable, RowTable

include("abstractrowtable.jl")
include("rowtable.jl")

end  # module RowTables
