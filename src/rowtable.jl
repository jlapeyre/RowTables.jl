### RowTable

mutable struct RowTable <: AbstractRowTable
    rows::Array{Any}
    colindex::CIndex
end

##############################################################################

### Access

@inline _index(rt::RowTable) = rt.colindex
@inline colindex(rt::RowTable,ci) = _index(rt)[ci]

"""
    rows(rt::RowTable)

Return the rows of `rt` as a Vector.
"""
@inline rows(rt::RowTable) = rt.rows

# Annotating the method above with return type Vector makes it 1000x slower in v0.6,
# and only a bit less slower in v0.7.
# So... TODO: remove return type annotations more or less everywhere
# @inline rows(rt::RowTable)::Vector = rt.rows

Base.names(rt::RowTable) = _names(_index(rt))
_names(rt::RowTable) = _names(_index(rt))
Base.size(rt::RowTable) =   _numberofrows(rt), _numberofcols(rt)
_numberofcols(rt::RowTable) = length(_names(rt))
_numberofrows(rt::RowTable) = length(rows(rt)) # should be the same as below
#_numberofrows(rt::RowTable) = isempty(rows(rt)) ? 0 : length(rows(rt))

## TODO: make sure this is optimized if n is known at compile time
## (And even if not known)
Base.size(rt::RowTable,n::Integer) = n == 1 ? _numberofrows(rt) : n == 2 ? _numberofcols(rt) :
    error(ArgumentError, ": RowTables have only two dimensions")

### Equality

Base.:(==)(rt1::RowTable, rt2::RowTable) = (_index(rt1) == _index(rt2) && rows(rt1) == rows(rt2))

### Constructors

newrows(n::Integer=0) = Vector{Any}(uninitialized, n)

# emtpy RowTable
RowTable() = RowTable(newrows(),CIndex())

## We did not use the type information afterall.

if VERSION >=  v"0.7.0-DEV"
     const _NameTypes = Union{AbstractVector{S} where S<:Union{Symbol,AbstractString}}
else
    const _NameTypes = Union{AbstractVector{S} where S<:Union{Symbol,AbstractString},
                         Base.KeyIterator{T} where T<:AbstractDict{V} where V <: Union{W,Symbol} where W <: AbstractString}
end

# @inbounds on a function does nothing
function _RowTable(a,keynames)
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

# v0.7 requires collect (or something else) here to avoid constructing a Set, which prevents indexing
_RowTable(::Type{T} , a::AbstractVector) where T <: AbstractDict  = _RowTable(T, a, collect(keys(first(a))))

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
#    arr = Vector{Any}(uninitialized,nr)
    arr = newrows()
    if tuples
      @inbounds   for ri in 1:nr
#          arr[ri] = ([df[ri,ci] for ci in 1:nc]...,)
            push!(arr, ([df[ri,ci] for ci in 1:nc]...,))
        end
    else
      @inbounds for ri in 1:nr
#          arr[ri] = [df[ri,ci] for ci in 1:nc]
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


##############################################################################
##
## Indexing
##
##############################################################################

##############################################################################
##
## getindex()
##
##############################################################################

## This allows single methods to handle both Int and Symbol indices.
## But, it allows repeated mapping of the same Symbol, which is inefficient.
## So, this might not be used much
const ColInd = Union{Integer,Symbol}

## A single index is a interpreted as a column index, consistent with DataFrames
Base.getindex(rt::RowTable,cinds) = rt[:,cinds]

## Return element in a single cell
Base.getindex(rt::RowTable,ri::Integer, ci::Symbol) = rows(rt)[ri][_index(rt)[ci]]

# If above is called in a loop with symbol arg, using below is faster

Base.getindex(rt::RowTable,ri::Integer, ci::Integer) = rows(rt)[ri][ci]


## Return a slice of a column as a Vector
function Base.getindex(rt::RowTable,ri::AbstractVector,ci::ColInd)
    ind = colindex(rt,ci) # do this so symbol mapping is only done once
    [rt[i,ind] for i in ri]
end

## Return a slice of a row as a Vector
Base.getindex(rt::RowTable, ri::Integer, cis::AbstractVector{T}) where T<:Symbol =
    rows(rt)[ri][[_index(rt)[ci] for ci in cis]]

### Return a row as a Vector
Base.getindex(rt::RowTable, ri::Integer, ::Colon) = rt.rows[ri]

## Return slice as RowTable
## Following method calls the next method with integer arguments
Base.getindex(rt::RowTable,ri::AbstractVector{T}, ci::AbstractVector{V}) where {T<:Integer,V<:Symbol} =
    Base.getindex(rt,ri, [_index(rt)[s] for s in ci])

## Return rectangular slice in both dimensions as RowTable
function Base.getindex(rt::RowTable,ri::AbstractVector, ci::AbstractVector{T}) where T<:Integer
    ar = newrows(length(ri))
    for (i,ind) in enumerate(ri)
        ar[i] = rows(rt)[ind][ci]
    end
    RowTable(ar, CIndex(_index(rt).names[ci]))
end

#Base.getindex(rt::RowTable, ::Colon, ci) = Base.getindex(rt, 1:length(rt.rows), ci)
Base.getindex(rt::RowTable, ::Colon, ci) = rt[1:length(rows(rt)),ci]

Base.getindex(rt::RowTable, ri::AbstractVector, ::Colon) = RowTable(rows(rt)[ri], rt.colindex)

##############################################################################
##
## setindex!()
##
##############################################################################

## Set single element
Base.setindex!(rt::RowTable, val, ri::Integer, ci::Integer) = (rows(rt)[ri][ci] = val)
Base.setindex!(rt::RowTable, val, ri::Integer, ci::Symbol) = (rows(rt)[ri][_index(rt)[ci]] = val)


### Convert

"""
    columns(rt::RowTable)::Vector

Return the columns of `rt`.
"""
## TODO: should the columns have more efficient types ?
## Yes, because this is used to construct DataFrames
## But, attempts to determine eltype by scanning are very slow
## This is much slower than converting in the other direction.
function columns(rt::RowTable)
    (nr,nc) = size(rt)
    @inbounds colarr =  [newrows(nr) for i in 1:nc] # misusing newrows for columns here
    return _columns!(rt,colarr)
end

function columnstyped(rt::RowTable)
    (nr,nc) = size(rt)
    @inbounds colarr =  [Vector{typeof(rt[1,i])}(uninitialized,nr) for i in 1:nc]
    return _columns!(rt,colarr)
end

## factoring this out makes columnstyped > 10% slower for a test case,
## if not prefixed with @inline
@inline function _columns!(rt, colarr)
    (nr,nc) = size(rt)
    for rowind in 1:nr
      @inbounds row = rows(rt)[rowind]
        for colind in 1:nc
        @inbounds colarr[colind][rowind] = row[colind]
        end
    end
    return colarr
end

## This is slow, so we don't use it
function _coltype(rt::RowTable, cind::Int)
    nr = size(rt,1)
    nr == 0 && return Any
    t = typeof(rt[1,cind])
    onetype::Bool = true
    for rowind in 2:nr
        if ! isa(rt[rowind,cind],t)
            onetype = false
            break
        end
    end
    return col = (onetype ? t : Any)
end

"""
    DataFrame(rt::RowTable; typed=false)

Convert `rt` to a `DataFrame`. If `typed` is `true`,
then the eltype of each column is the type of the first
element in the column. This will raise an error if the
elements are in fact not of the same type.
"""
function DataFrames.DataFrame(rt::RowTable; typed=false)
    cols = (typed ? columnstyped(rt) : columns(rt))
    DataFrames.DataFrame(cols,_names(rt))
end

### Copy

Base.copy(rt::RowTable) = RowTable(copy(rows(rt)), copy(_index(rt)))
Base.deepcopy(rt::RowTable) = RowTable(deepcopy(rows(rt)), deepcopy(_index(rt)))

### Iterate over rows
## DataFrames does not define iterating over a DataFrame.
## We do for RowTable

for f in (:length, :start, :endof)
    @eval begin
        (Base.$f)(rt::RowTable) = (Base.$f)(rows(rt))
    end
end

for f in (:next, :done)
    @eval begin
        (Base.$f)(rt::RowTable,args...) = (Base.$f)(rows(rt),args...)
    end
end

### Transform

for f in (:deleteat!, :push!, :insert!, :unshift!, :append!, :prepend!, :splice!, :permute!)
    @eval begin
        (Base.$f)(rt::RowTable,args...) = (($f)(rows(rt),args...); rt)
    end
end

for f in (:pop!,:shift!)
    @eval begin
        (Base.$f)(rt::RowTable,args...) = ($f)(rows(rt),args...)
    end
end

## These are needed so that our methods are called, and not generic Base methods.
Base.permute!(rt::RowTable,p::AbstractVector) = (permute!(rows(rt),p); rt)
Base.permute(rt::RowTable,p::AbstractVector) = permute!(copy(rt),p)

Base.shuffle!(rng::AbstractRNG, rt::RowTable) = (shuffle!(rng,rows(rt)); rt)
Base.shuffle!(rt::RowTable) = (shuffle!(rows(rt)); rt)
Base.shuffle(rt::RowTable) = shuffle!(copy(rt))
Base.shuffle(rng::AbstractRNG,rt::RowTable) = shuffle!(rng,copy(rt))

DataFrames.rename!(rt::RowTable,d) = (rename!(_index(rt),d); rt)
DataFrames.rename(rt::RowTable,d) = rename!(copy(rt),d)
