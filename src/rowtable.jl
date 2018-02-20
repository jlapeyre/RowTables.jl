### CIndex

"""
    CIndex

Data holding column names and map to linear indices for RowTable.
"""
struct CIndex
    names::Array{Symbol}
    map::Dict{Symbol,Int}
end

CIndex(names::Array{Symbol}) = CIndex(names,Dict( s => i for (i,s) in enumerate(names)))

Base.:(==)(c1::CIndex, c2::CIndex) = (c1.names == c2.names)

### RowTable

mutable struct RowTable
    rows::Array{Any}
    colindex::CIndex
end

RowTable() = RowTable(Any[],CIndex(Symbol[]))

## We did not use the type information afterall.
const _NameTypes = Union{AbstractVector{S} where S<:Union{Symbol,AbstractString},
                         Base.KeyIterator{T} where T<:AbstractDict{V} where V <: Union{W,Symbol} where W <: AbstractString}

function _RowTable(a,keynames)
    isempty(a) && return RowTable(Any[Any[]],CIndex(map(Symbol,keynames)))
    l = length(first(a))
    all(x -> length(x) == l, a) || throw(DiminsionMismatch("All dictionaries must be of the same length"))
    RowTable([map(x -> a[i][x],keynames) for i in linearindices(a)], CIndex(map(Symbol,keynames)))
end

RowTable(a::AbstractVector{T},keynames::_NameTypes) where {T<:AbstractDict} = _RowTable(a,keynames)

function RowTable(a::AbstractVector{T}) where {T<:AbstractDict}
    isempty(a) && return RowTable()
    RowTable(a,keys(first(a)))
end

function RowTable(a::AbstractVector,keynames::_NameTypes)
    isempty(a) && return RowTable()
    all(x -> isa(x,AbstractDict), a) || error("Not all elements are dictionaries")
    _RowTable(a,keynames)
end

function RowTable(a::AbstractVector)
    isempty(a) && return RowTable()
    all(x -> isa(x,AbstractDict), a) || error("Not all elements are dictionaries")
    _RowTable(a,keys(first(a)))
end

Base.:(==)(rt1::RowTable, rt2::RowTable) = (rt1.colindex == rt2.colindex && rt1.rows == rt2.rows)

### Info

function Base.size(rt::RowTable)
    _numberofrows(rt), _numberofcols(rt)
end

_numberofcols(rt::RowTable) = length(rt.colindex.names)
_numberofrows(rt::RowTable) = isempty(rt.rows) ? 0 : length(rt.rows)

Base.size(rt::RowTable,n::Integer) = n == 1 ? _numberofrows(rt) : n == 2 ? _numberofcols(rt) :
    error(ArgumentError, ": RowTables have only two dimensions")

Base.length(rt::RowTable) = _numberofcols(rt)

Base.names(rt::RowTable) = rt.colindex.names

function Base.summary(rt::RowTable) # -> String
    nrows, ncols = size(rt)
    return @sprintf("%d×%d %s", nrows, ncols, typeof(rt))
end

### Index

### One row returned

"""
    getindex(rt::RowTable,ind::Integer)

A single index returns a row, not a column as in `DataFrames`.
"""
Base.getindex(rt::RowTable,ind::Integer) = rt.rows[ind]

### Single cell returned

"""
    getindex(rt::RowTable,ri::Integer, ci::Integer)

Return a single cell.
"""
Base.getindex(rt::RowTable,ri::Integer, ci::Integer) = rt.rows[ri][ci]

"""
    getindex(rt::RowTable,ri::Integer, ci::Symbol)

Return a single cell.
"""
Base.getindex(rt::RowTable,ri::Integer, ci::Symbol) = rt.rows[ri][rt.colindex.map[ci]]

### RowTable returned

"""
    getindex(rt::RowTable,inds)

Return a `RowTable` with rows specified by `inds`.
"""
Base.getindex(rt::RowTable,inds) = RowTable(rt.rows[inds],rt.colindex)

Base.getindex(rt::RowTable,ri::AbstractVector, ci::Integer) = getindex(rt,ri,[ci])
Base.getindex(rt::RowTable,ri::AbstractVector{T}, ci::Symbol) where T<:Integer = rt.rows[ri][rt.colindex.map[ci]]
Base.getindex(rt::RowTable,ri::Integer, ci::AbstractVector{T}) where T<:Symbol = rt.rows[ri][[rt.colindex.map[i] for i in ci]]
Base.getindex(rt::RowTable,ri::AbstractVector{T}, ci::AbstractVector{V}) where {T<:Integer,V<:Symbol} =
    Base.getindex(rt,ri, [rt.colindex.map[i] for i in ci])

function Base.getindex(rt::RowTable,ri::AbstractVector, ci::AbstractVector{T}) where T<:Integer
    ar = Any[]
    for i in ri
        push!(ar, rt.rows[i][ci])
    end
    RowTable(ar, CIndex(rt.colindex.names[ci]))
end

Base.getindex(rt::RowTable, ::Colon, ci) = Base.getindex(rt, 1:length(rt.rows), ci)

function Base.getindex(rt::RowTable, ri::AbstractVector, ::Colon)
    RowTable(rt.rows[ri], rt.colindex)
end

### IO

function Base.show(io::IO, rt::RowTable, allcols::Bool=false, displaysummary::Bool=true)
    df = DataFrames.DataFrame(rt)
    rowlabel= :Row
    dfdisplaysummary = false
    displaysummary && print(io, summary(rt))
    show(io,
         df,
         allcols,
         rowlabel,
         dfdisplaysummary)
end

function Base.show(rt::RowTable,
                   allcols::Bool = false) # -> Void
    return show(STDOUT, rt, allcols)
end

### Convert

function tocolumns(rt::RowTable)
    (nr,nc) = size(rt)
    arr = [Vector{Any}(nr) for i in 1:nc]
    for rowind in 1:nr
        row = rt.rows[rowind]
        for colind in 1:nc
            arr[colind][rowind] = row[colind]
        end
    end
    return arr
end

DataFrames.DataFrame(rt::RowTable) = DataFrames.DataFrame(tocolumns(rt),names(rt))

### Transform

Base.deleteat!(rt::RowTable,ind::Integer) = (deleteat!(rt.rows,ind), rt)

function Base.deleteat!(rt::RowTable,inds)
    deleteat!(rt.rows,inds)
    return rt
end
