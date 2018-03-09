### CIndex

"""
    CIndex

Data holding column names and map to linear indices for RowTable.
"""
struct CIndex
    names::Array{Symbol}
    map::Dict{Symbol,Int}
end

"""
    Base.names(c::CIndex) = c.names

Return the names of the index `c`.
"""
Base.names(c::CIndex) = c.names

@inline _names(c::CIndex) = c.names
@inline _map(c::CIndex) = c.map

### CIndex constructors

CIndex() = CIndex(Symbol[])::CIndex

"""
    CIndex(names::Vector{Symbol})::CIndex

Create a `Cindex` with names and their ordinal position as in `names`.
"""
CIndex(names::Vector{Symbol})::CIndex = CIndex(names, Dict(s => i for (i,s) in enumerate(names)))

# Hack around unknown bug that shows up only in v0.7
CIndex(names::Set{Symbol})::CIndex = CIndex([x for x in names])

Base.:(==)(c1::CIndex, c2::CIndex) = (_names(c1) == _names(c2))

@inline Base.getindex(c::CIndex, inds) = inds
@inline Base.getindex(c::CIndex, s::Symbol)::Int = _map(c)[s]
# can we avoid allocation by returning an iterator ?
Base.getindex(c::CIndex, syms::AbstractVector{T}) where T <: Symbol = [_map(c)[s] for s in syms]

### Copy

Base.copy(ci::CIndex) = CIndex(copy(_names(ci)),copy(ci.map))
Base.deepcopy(ci::CIndex) = CIndex(deepcopy(_names(ci)),deepcopy(ci.map))

### Rename

DataFrames.rename!(c::CIndex, a::Array{T}) where T <: Pair = rename!(c::CIndex,Dict(a))

function DataFrames.rename!(c::CIndex, d::AbstractDict)
    newnames = copy(c.names)
    for (from,to) in d
        haskey(c.map, from) || throw(ErrorException("There is no existing name $from"))
        newnames[c.map[from]] =  to
    end
    length(newnames) == length(unique(newnames)) || throw(ArgumentError("names must be unique"))
    for (i,k) in enumerate(newnames)
        c.names[i] = k
        c.map[k] = i
    end
end
