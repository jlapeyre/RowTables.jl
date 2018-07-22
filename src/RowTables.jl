__precompile__()

module RowTables

import DataFrames
import DataFrames.rename!
import DataStructures.OrderedDict
import Random
import JSON
using Printf

export AbstractRowTable, RowTable, rows, columns, rowdict

include("index.jl")
include("abstractrowtable.jl")
include("rowtable.jl")
include("display.jl")
include("sort.jl")

end  # module RowTables
