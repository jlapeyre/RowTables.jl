# RowTables
*Row-wise data table*

Linux, OSX: [![Build Status](https://travis-ci.org/jlapeyre/RowTables.jl.svg)](https://travis-ci.org/jlapeyre/RowTables.jl)
&nbsp;
Windows: [![Build Status](https://ci.appveyor.com/api/projects/status/github/jlapeyre/RowTables.jl?branch=master&svg=true)](https://ci.appveyor.com/project/jlapeyre/rowtables-jl)
&nbsp; &nbsp; &nbsp;
[![Coverage Status](https://coveralls.io/repos/jlapeyre/RowTables.jl/badge.svg?branch=master&service=github)](https://coveralls.io/github/jlapeyre/RowTables.jl?branch=master)
[![codecov.io](http://codecov.io/github/jlapeyre/RowTables.jl/coverage.svg?branch=master)](http://codecov.io/github/jlapeyre/RowTables.jl?branch=master)

This package provides a data structure that is very similar to [`DataFrames.jl`](https://github.com/JuliaData/DataFrames.jl), but
stores data differently. While `DataFrames.jl` stores data as columns, `RowTables.jl` stores them as rows. Just as the columns in
`DataFrames.jl` may be of heterogeneous types, the rows in `RowTables.jl` may be of different types, for instance, `Vector`s or
named `Tuples`.

Quit a bit is implemented, with a focus on operations that are likely more efficient when data is stored in rows. However,
after testing I have not found a practical use case that compensates for the time required to convert between `DataFrames` and
`RowTables`. So, this package is not being developed at the moment.


