### CIndex

"""
    CIndex

Data holding column names and map to linear indices for RowTable.
"""
struct CIndex
    names::Array{Symbol}
    map::Dict{Symbol,Int}
end

Base.names(c::CIndex) = c.names

_names(c::CIndex) = c.names
_map(c::CIndex) = c.map

CIndex() = CIndex(Symbol[])
CIndex(names::Array{Symbol}) = CIndex(names, Dict(s => i for (i,s) in enumerate(names)))

Base.:(==)(c1::CIndex, c2::CIndex) = (_names(c1) == _names(c2))

Base.getindex(c::CIndex, inds) = inds
Base.getindex(c::CIndex, s::Symbol) = _map(c)[s]
# can avoid allocation by returning an iterator ?
Base.getindex(c::CIndex, syms::AbstractVector{T}) where T <: Symbol = [_map(c)[s] for s in syms] 

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
