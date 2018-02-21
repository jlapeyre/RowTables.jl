### RowTable

mutable struct RowTable <: AbstractRowTable
    rows::Array{Any}
    colindex::CIndex
end

##############################################################################

index(rt::RowTable) = rt.colindex
rows(rt::RowTable) = rt.rows
Base.names(rt::RowTable) = _names(index(rt))
_names(rt::RowTable) = _names(index(rt))
Base.length(rt::RowTable) = _numberofcols(rt)
Base.size(rt::RowTable) =   _numberofrows(rt), _numberofcols(rt)
_numberofcols(rt::RowTable) = length(_names(rt))
_numberofrows(rt::RowTable) = isempty(rows(rt)) ? 0 : length(rows(rt))

Base.size(rt::RowTable,n::Integer) = n == 1 ? _numberofrows(rt) : n == 2 ? _numberofcols(rt) :
    error(ArgumentError, ": RowTables have only two dimensions")

newrows(n::Integer=0) = Vector{Any}(n)

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

_RowTable(::Type{T} , a::AbstractVector) where T <: AbstractDict  = _RowTable(T, a, keys(first(a)))

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

Base.:(==)(rt1::RowTable, rt2::RowTable) = (index(rt1) == index(rt2) && rows(rt1) == rows(rt2))
#Base.:(==)(rt1::RowTable, rt2::RowTable) = (rt1.colindex == rt2.colindex && rt1.rows == rt2.rows)

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

Base.getindex(rt::RowTable,ri::Integer, ci::ColInd) = rows(rt)[ri][index(rt)[ci]]

### RowTable returned

Base.getindex(rt::RowTable,inds) = RowTable(rows(rt)[inds],index(rt)) # copy index ?

# Not what we want
Base.getindex(rt::RowTable,ri::AbstractVector, ci::Integer) = getindex(rt,ri,[ci])

Base.getindex(rt::RowTable,ri::AbstractVector{T}, ci::Symbol) where T<:Integer = rt.rows[ri][rt.colindex.map[ci]]
Base.getindex(rt::RowTable,ri::Integer, ci::AbstractVector{T}) where T<:Symbol = rt.rows[ri][[rt.colindex.map[i] for i in ci]]
Base.getindex(rt::RowTable,ri::AbstractVector{T}, ci::AbstractVector{V}) where {T<:Integer,V<:Symbol} =
    Base.getindex(rt,ri, [rt.colindex.map[i] for i in ci])

function Base.getindex(rt::RowTable,ri::AbstractVector, ci::AbstractVector{T}) where T<:Integer
    ar = newrows(length(ri))
    for i in ri
        ar[i] = rt.rows[i][ci]
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

### Transform

## These only work with integer indices
for f in (:deleteat!, :push!, :insert!, :unshift!, :shift!, :pop!, :append!, :prepend!, :splice!)
    @eval begin
        (Base.$f)(rt::RowTable,args...) = (($f)(rows(rt),args...); rt)
    end
end


DataFrames.rename!(rt::RowTable,d) = (rename!(index(rt),d); rt)
