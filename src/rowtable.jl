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
    rows(rt::RowTable)::Vector

Return the rows of `rt`.
"""
@inline rows(rt::RowTable)::Vector = rt.rows
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
## Index
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
Base.getindex(rt::RowTable, ri::Integer, ci::AbstractVector{T}) where T<:Symbol =
    rt.rows[ri][[rt.colindex.map[i] for i in ci]]

### Return a row as a Vector
Base.getindex(rt::RowTable, ri::Integer, ::Colon) = rt.rows[ri]

## Return slice as RowTable
## Following method calls the next method with integer arguments
Base.getindex(rt::RowTable,ri::AbstractVector{T}, ci::AbstractVector{V}) where {T<:Integer,V<:Symbol} =
    Base.getindex(rt,ri, [_index(rt).map[s] for s in ci])

## Return rectangular slice in both dimensions as RowTable
function Base.getindex(rt::RowTable,ri::AbstractVector, ci::AbstractVector{T}) where T<:Integer
    ar = newrows(length(ri))
    for (i,ind) in enumerate(ri)
        ar[i] = rt.rows[ind][ci]
    end
    RowTable(ar, CIndex(rt.colindex.names[ci]))
end

#Base.getindex(rt::RowTable, ::Colon, ci) = Base.getindex(rt, 1:length(rt.rows), ci)
Base.getindex(rt::RowTable, ::Colon, ci) = rt[1:length(rows(rt)),ci]

Base.getindex(rt::RowTable, ri::AbstractVector, ::Colon) = RowTable(rows(rt)[ri], rt.colindex)

### Iterate over rows

for f in (:length, :start)
    @eval begin
        (Base.$f)(rt::RowTable) = (Base.$f)(rows(rt))
    end
end

for f in (:next, :done)
    @eval begin
        (Base.$f)(rt::RowTable,args...) = (Base.$f)(rows(rt),args...)
    end
end


### IO

## Lazy approach: For displaying, convert to DataFrame and display with different header

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

"""
    columns(rt::RowTable)::Vector

Return the columns of `rt`.
"""
## TODO: should the columns have more efficient types ?
## Yes, because this is used to construct DataFrames
function columns(rt::RowTable)::Vector
    (nr,nc) = size(rt)
    @inbounds colarr =  [newrows(nr) for i in 1:nc]
    for rowind in 1:nr
      @inbounds row = rows(rt)[rowind]
        for colind in 1:nc
        @inbounds colarr[colind][rowind] = row[colind]
        end
    end
    return colarr
end

function coltype(rt::RowTable, cind::Int)
    nr = size(rt,1)
    nr == 0 && return Any
    t = typeof(rt[1,cind])
    onetype::Bool = true
    for rowind in 2:nr
        if ! isa(rt[rowind,ci],t)
            onetype = false
            break
        end
    end
    return col = (onetype ? t : Any)
    # col = onetype ? Vector{t}(uninitialized,nr) : Vector{Any}(uninitialized,nr)
    #     for colind in 1:nc
    #         col[colind] = row[rowind]
    #     end
    #     push!(colarr,col)
end

## As expected, this is much slower than `columns`
function columns2(rt::RowTable)::Vector
    (nr,nc) = size(rt)
    @inbounds colarr =  [newrows(nr) for i in 1:nc]
    for colind in 1:nc
        col = colarr[colind] # does nothing to increase speed (as expected)
        for rowind in 1:nr
            @inbounds col[rowind] = rows(rt)[rowind][colind]
        end
    end
    return colarr
end


# function columns2(rt::RowTable)::Vector
#     (nr,nc) = size(rt)
#     colarr =  Any[]
#     for rowind in 1:nr
#         @inbounds row = rows(rt)[rowind]
#         t = typeof(row[1])
#         onetype::Bool = true
#         for colind in 2:nc
#             if ! isa(row[colind],t)
#                 onetype = false
#                 break
#             end
#         end
#         col = onetype ? Vector{t}(uninitialized,nr) : Vector{Any}(uninitialized,nr)
#         for colind in 1:nc
#             col[colind] = row[rowind]
#         end
#         push!(colarr,col)
#     end
#     return colarr
# end



DataFrames.DataFrame(rt::RowTable) = DataFrames.DataFrame(columns(rt),_names(rt))

struct RowDict{T}
    dict::OrderedDict{T}
end

struct RowArr{T}
    arr::Vector{T}
end

"""
    rowdict(rt::RowTable, rowind::Integer)::RowDict

Return the `rowind`th row from `rt` wrapped so that it is pretty printed.
"""
function rowdict(rt::RowTable, rowind::Integer)
    od = OrderedDict{Symbol,Any}()
    for (colind::Integer,cname::Symbol) in enumerate(_names(rt))
        od[cname] = rows(rt)[rowind][colind]
    end
    RowDict(od)
end


"""
    rowdict(rt::RowTable, rowinds::AbstractVector)::RowDict

Return the rows indexed by `rowinds` from `rt` wrapped so that they are pretty printed.
"""
function rowdict(rt::RowTable, rowinds::AbstractVector)::RowArr
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

### Copy

Base.copy(rt::RowTable) = RowTable(copy(rows(rt)), copy(_index(rt)))
Base.deepcopy(rt::RowTable) = RowTable(deepcopy(rows(rt)), deepcopy(_index(rt)))

### Transform

for f in (:deleteat!, :push!, :insert!, :unshift!, :shift!, :pop!, :append!, :prepend!, :splice!, :permute!)
    @eval begin
        (Base.$f)(rt::RowTable,args...) = (($f)(rows(rt),args...); rt)
    end
end

Base.permute!(rt::RowTable,p::AbstractVector) = (permute!(rows(rt),p); rt)
Base.shuffle!(rng::AbstractRNG, rt::RowTable) = (shuffle!(rng,rows(rt)); rt)
Base.shuffle!(rt::RowTable) = (shuffle!(rows(rt)); rt)


DataFrames.rename!(rt::RowTable,d) = (rename!(_index(rt),d); rt)
