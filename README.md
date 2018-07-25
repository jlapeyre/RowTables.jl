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

Quite a bit is implemented, with a focus on operations that are likely more efficient when data is stored in rows.

### Examples

Create a `RowTable` from a `Dict`
```julia
julia> using RowTables; using DataFrames; using BenchmarkTools

julia> ds = [
           Dict(:a=>2,:b=>1,:c=>7)
           Dict(:a=>1,:b=>4,:c=>8)
           Dict(:a=>9,:b=>6,:c=>4)
           Dict(:a=>10,:b=>8,:c=>7)
           Dict(:a=>10,:b=>4,:c=>3)];

julia> rt = RowTable(ds)
5×3 RowTable
│ Row │ a  │ b │ c │
├─────┼────┼───┼───┤
│ 1   │ 2  │ 1 │ 7 │
│ 2   │ 1  │ 4 │ 8 │
│ 3   │ 9  │ 6 │ 4 │
│ 4   │ 10 │ 8 │ 7 │
│ 5   │ 10 │ 4 │ 3 │
```

Convert `RowTable` to `DataFrame`.

```julia
julia> df = DataFrame(rt)
5×3 DataFrame
│ Row │ a  │ b │ c │
├─────┼────┼───┼───┤
│ 1   │ 2  │ 1 │ 7 │
│ 2   │ 1  │ 4 │ 8 │
│ 3   │ 9  │ 6 │ 4 │
│ 4   │ 10 │ 8 │ 7 │
│ 5   │ 10 │ 4 │ 3 │
```

Time converting between the two.
```julia
julia> @btime DataFrame(rt);
  2.614 μs (27 allocations: 2.31 KiB)

julia> @btime RowTable(df);
  4.606 μs (48 allocations: 2.83 KiB)
```

Getting a single row is (in this case) faster for the `RowTable`
than the `DataFrame`.
```julia
julia> @btime rt[3:3,:];
  82.403 ns (4 allocations: 192 bytes)

julia> @btime df[3,:]
  3.697 μs (36 allocations: 2.64 KiB)
```

Getting a single column is faster with `DataFrame`.
```julia
julia> @btime df[:,3]
  30.714 ns (0 allocations: 0 bytes)
  
julia> @btime rt[:,3]
  367.115 ns (4 allocations: 224 bytes)
```

Indexing a single row of a `DataFrame` returns a `DataFrame`,
while indexing a single column returns the column data.
```julia
julia> df[3,:]
1×3 DataFrame
│ Row │ a │ b │ c │
├─────┼───┼───┼───┤
│ 1   │ 9 │ 6 │ 4 │

julia> df[:,3]
5-element Array{Any,1}:
 7
 8
 4
 7
 3
```

With `RowTable`, indexing a single row or column is symmetric.
```julia
julia> rt[3,:]
3-element Array{Int64,1}:
 9
 6
 4

julia> rt[:,3]
5-element Array{Int64,1}:
 7
 8
 4
 7
 3
```

This is how to get a single row as a `RowTable`.
```julia
julia> rt[3:3,:]
1×3 RowTable
│ Row │ a │ b │ c │
├─────┼───┼───┼───┤
│ 1   │ 9 │ 6 │ 4 │
```

### 100x100 tables

Here are 100x100 data tables with random elements.
```julia
function mkbigdf(nr,nc)
    cols = Any[]
    for i in 1:nc
        push!(cols,rand(nr))
    end
    syms = [Symbol("x",i) for i in 1:nc]
    DataFrame(cols,syms)
end

julia> dfb = mkbigdf(100,100);
julia> rtb = RowTable(dfb);
```

Time getting row and column slices.
```julia
julia> @btime rtb[50:55, :];
  81.895 ns (4 allocations: 224 bytes)

julia> @btime dfb[50:55, :];
  45.124 μs (315 allocations: 27.88 KiB)

julia> @btime dfb[:, 50:55];
  3.253 μs (29 allocations: 2.20 KiB)

julia> @btime rtb[:, 50:55];
  7.407 μs (211 allocations: 17.34 KiB)
```

Sorting rows on one column is faster with `RowTable`
```julia
function randsort!(obj::RowTable)
    sort!(obj, [rand(1:size(obj,2))])
    nothing
end

function randsort!(obj::DataFrame)
    sort!(obj, [rand(1:size(obj,2))])
    nothing
end

julia> @btime randsort!(dfb);
  129.140 μs (915 allocations: 29.27 KiB)

julia> @btime randsort!(rtb);
  6.929 μs (248 allocations: 4.44 KiB)
```

Time converting between the two.
```
julia> @btime DataFrame(rtb);
  260.766 μs (10233 allocations: 262.75 KiB)

julia> @btime RowTable(dfb);
  645.358 μs (10723 allocations: 277.91 KiB)
```

Conversion of these 100x100 tables is more expensive than creating the tables.
```julia
julia> @btime mkbigdf(100,100);
  168.249 μs (939 allocations: 140.70 KiB)
```

