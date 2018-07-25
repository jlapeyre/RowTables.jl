### CIndex

"""
    CIndex

Data holding column names and map to linear indices for RowTable.
"""
struct CIndex
    names::Vector{Symbol}
    smap::Dict{Symbol, Int}
end

"""
    Base.names(c::CIndex) = c.names

Return the names of the index `c`.
"""
Base.names(c::CIndex) = c.names

@inline _names(c::CIndex) = c.names
@inline _smap(c::CIndex) = c.smap

### CIndex constructors

CIndex() = CIndex(Symbol[])

"""
    CIndex(names::Vector{Symbol})::CIndex

Create a `Cindex` with names and their ordinal position as in `names`.
"""
CIndex(names::Vector{Symbol}) = CIndex(names, Dict(s => i for (i, s) in enumerate(names)))

# Hack around unknown bug that shows up only in v0.7
CIndex(names::Set{Symbol}) = CIndex([x for x in names])

Base.:(==)(c1::CIndex, c2::CIndex) = (_names(c1) == _names(c2))

@inline Base.getindex(c::CIndex, inds) = inds
@inline Base.getindex(c::CIndex, s::Symbol) = _smap(c)[s]  # returns Int
# can we avoid allocation by returning an iterator ?
@inline Base.getindex(c::CIndex, syms::AbstractVector{T}) where T <: Symbol = [_smap(c)[s] for s in syms]
@inline Base.getindex(c::CIndex, inds::AbstractVector{T}) where T <: Integer = inds
@inline Base.getindex(c::CIndex, inds::AbstractVector) = [isa(s, Symbol) ?  _smap(c)[s] : s  for s in inds]

### Copy

@inline Base.copy(ci::CIndex) = CIndex(copy(ci.names), copy(ci.smap))
Base.deepcopy(ci::CIndex) = CIndex(deepcopy(_names(ci)), deepcopy(ci.smap))

### Rename

DataFrames.rename!(c::CIndex, a::Array{T}) where T <: Pair = rename!(c::CIndex, Dict(a))

function DataFrames.rename!(c::CIndex, d::AbstractDict)
    newnames = copy(_names(c))
    for (from, to) in d
        haskey(_smap(c), from) || throw(ErrorException("There is no existing name $from"))
        newnames[_smap(c)[from]] =  to
    end
    length(newnames) == length(unique(newnames)) || throw(ArgumentError("names must be unique"))
 @inbounds for (i, k) in enumerate(newnames)
        _names(c)[i] = k
        _smap(c)[k] = i
    end
end
