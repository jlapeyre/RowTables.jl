## What part of the interface needs to be abstract ?

abstract type AbstractRowTable end

Base.names(rt::AbstractRowTable) = names(index(rt))
