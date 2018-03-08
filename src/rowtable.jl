### RowTable

mutable struct RowTable <: AbstractRowTable
    rows::Array{Any}
    colindex::CIndex
end

##############################################################################

### Access

@inline _index(rt::RowTable) = rt.colindex
@inline colindex(rt::RowTable,ci) = _index(rt)[ci]
#_index(rt::RowTable) = rt.colindex
@inline rows(rt::RowTable) = rt.rows
Base.names(rt::RowTable) = _names(_index(rt))
_names(rt::RowTable) = _names(_index(rt))
Base.length(rt::RowTable) = _numberofcols(rt)
Base.size(rt::RowTable) =   _numberofrows(rt), _numberofcols(rt)
_numberofcols(rt::RowTable) = length(_names(rt))
_numberofrows(rt::RowTable) = isempty(rows(rt)) ? 0 : length(rows(rt))

Base.size(rt::RowTable,n::Integer) = n == 1 ? _numberofrows(rt) : n == 2 ? _numberofcols(rt) :
    error(ArgumentError, ": RowTables have only two dimensions")

### Basic ops

Base.:(==)(rt1::RowTable, rt2::RowTable) = (_index(rt1) == _index(rt2) && rows(rt1) == rows(rt2))

### Constructors

newrows(n::Integer=0) = Vector{Any}(uninitialized, n)

RowTable() = RowTable(newrows(),CIndex())

## We did not use the type information afterall.

if VERSION >=  v"0.7.0-DEV"
     const _NameTypes = Union{AbstractVector{S} where S<:Union{Symbol,AbstractString}}
else
    const _NameTypes = Union{AbstractVector{S} where S<:Union{Symbol,AbstractString},
                         Base.KeyIterator{T} where T<:AbstractDict{V} where V <: Union{W,Symbol} where W <: AbstractString}
end

@inbounds function _RowTable(a,keynames)
    isempty(a) && return RowTable(newrows(),CIndex(map(Symbol,keynames))) # JSON keys are strings
    l = length(first(a))
    all(x -> length(x) == l, a) || throw(DiminsionMismatch("All dictionaries must be of the same length"))
    RowTable([map(x -> a[i][x],keynames) for i in linearindices(a)], CIndex(map(Symbol,keynames)))
end

RowTable(a::AbstractVector{T},keynames::_NameTypes) where {T<:AbstractDict} = _RowTable(a,keynames)

function _RowTable(::Type{T} , a::AbstractVector, keynames) where T <: AbstractArray
    all(x -> isa(x,AbstractArray), a) || error("Not all elements are arrays")  # They don't have to be. Just not dicts
    RowTable(a,CIndex(keynames))
end

function _RowTable(::Type{T} , a::AbstractVector, keynames) where T <: Tuple
    #all(x -> isa(x,AbstractArray), a) || error("Not all elements are arrays")  # They don't have to be. Just not dicts
    RowTable(a,CIndex(keynames))
end

# v0.7 requires collect (or something else) here to avoid constructing a Set, which preventd indexing
_RowTable(::Type{T} , a::AbstractVector) where T <: AbstractDict  = _RowTable(T, a, collect(keys(first(a))))
#_RowTable(::Type{T} , a::AbstractVector) where T <: AbstractDict  = _RowTable(T, a, keys(first(a)))


function _RowTable(::Type{T} , a::AbstractVector, keynames) where T <: AbstractDict
    all(x -> isa(x,AbstractDict), a) || error("Not all elements are dictionaries")
    _RowTable(a,keynames)
end

function RowTable(a::AbstractVector)
    isempty(a) && return RowTable()
    _RowTable(typeof(first(a)),a)
end

function RowTable(a::AbstractVector,keynames::_NameTypes)
    isempty(a) && return RowTable() # TODO fix this to take names
    _RowTable(typeof(first(a)),a,keynames)
end

function RowTable(df::DataFrames.DataFrame; tuples=false)
    (nr,nc) = size(df)
    arr = Any[]
    if tuples
        for ri in 1:nr
            push!(arr, ([df[ri,ci] for ci in 1:nc]...,))
        end
    else
        for ri in 1:nr
            push!(arr, [df[ri,ci] for ci in 1:nc])
        end
    end
    RowTable(arr, copy(names(df)))
end

### Info

function Base.summary(rt::RowTable) # -> String
    nrows, ncols = size(rt)
    return @sprintf("%d×%d %s", nrows, ncols, typeof(rt))
end

### Index

const ColInd = Union{Integer,Symbol}

### One row returned

Base.getindex(rt::RowTable,ind::Integer) = rows(rt)[ind]

### Single cell returned

Base.getindex(rt::RowTable,ri::Integer, ci::ColInd) = rows(rt)[ri][_index(rt)[ci]]
# If above is called in a loop with symbol arg, using below is faster
Base.getindex(rt::RowTable,ri::Integer, ci::Integer) = rows(rt)[ri][ci]

### RowTable returned

Base.getindex(rt::RowTable,inds) = RowTable(rows(rt)[inds],_index(rt)) # copy index ?

### Return a Vector
function Base.getindex(rt::RowTable,ri::AbstractVector,ci::ColInd)
    ind = colindex(rt,ci) # do this so symbol mapping is only done once
    [rt[i,ind] for i in ri]
end

### Return a Vector
Base.getindex(rt::RowTable, ri::Integer, ci::AbstractVector{T}) where T<:Symbol =
    rt.rows[ri][[rt.colindex.map[i] for i in ci]]

## Return slices as RowTable
## Following calls the next method with integer arguments
Base.getindex(rt::RowTable,ri::AbstractVector{T}, ci::AbstractVector{V}) where {T<:Integer,V<:Symbol} =
    Base.getindex(rt,ri, [_index(rt).map[s] for s in ci])

## Return slices as RowTable
function Base.getindex(rt::RowTable,ri::AbstractVector, ci::AbstractVector{T}) where T<:Integer
    ar = newrows(length(ri))
    for (i,ind) in enumerate(ri)
        ar[i] = rt.rows[ind][ci]
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

@inbounds function tocolumns(rt::RowTable)
    (nr,nc) = size(rt)
    arr = [newrows(nr) for i in 1:nc]
    for rowind in 1:nr
        row = rows(rt)[rowind]
        for colind in 1:nc
            arr[colind][rowind] = row[colind]
        end
    end
    return arr
end

DataFrames.DataFrame(rt::RowTable) = DataFrames.DataFrame(tocolumns(rt),_names(rt))

struct RowDict{T}
    dict::OrderedDict{T}
end

struct RowArr{T}
    arr::Vector{T}
end

function rowdict(rt::RowTable, rowind::Integer)
    od = OrderedDict{Symbol,Any}()
    for (colind::Integer,cname::Symbol) in enumerate(_names(rt))
        od[cname] = rows(rt)[rowind][colind]
    end
    RowDict(od)
end

function rowdict(rt::RowTable, rowinds::AbstractVector)
    ar = Any[]
    for rowind in rowinds
        push!(ar, rowdict(rt,rowind).dict)
    end
    RowArr(ar)
end


function Base.show(io::IO, rd::RowDict)
    indent = 4
    JSON.Writer.print(io,rd.dict,indent)
end

function Base.show(io::IO, ar::RowArr)
    indent = 4
    JSON.Writer.print(io,ar.arr,indent)
end


### Transform

for f in (:deleteat!, :push!, :insert!, :unshift!, :shift!, :pop!, :append!, :prepend!, :splice!)
    @eval begin
        (Base.$f)(rt::RowTable,args...) = (($f)(rows(rt),args...); rt)
    end
end

DataFrames.rename!(rt::RowTable,d) = (rename!(_index(rt),d); rt)
