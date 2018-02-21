abstract type AbstractRowTable end

Base.names(rt::AbstractRowTable) = names(index(rt))
