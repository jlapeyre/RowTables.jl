# Minimal implementation. Sort by columns

function _col_lt(ci...)
    function (x,y)
        for c in ci
            x[c] < y[c] && return true
            x[c] > y[c] && return false
        end
        return false
    end
end

function Base.sort!(rt::RowTable; cols=[], kws...)
    if isempty(cols)
        icols = collect(1:size(rt,2))
    else
        icols = _index(rt)[cols]
    end
    nc = length(icols)
    if nc == 1
        c = icols[1]
        #        Base.sort!(rows(rt); by= x->x[icols[1]], kws...)
        Base.sort!(rows(rt); lt = (x,y)->x[c]<y[c], kws...) # seems lt is faster than by
    else
        Base.sort!(rows(rt); lt=_col_lt(icols...), kws...)
    end
    return rt
end

Base.sort(rt::RowTable; kws...) = sort!(copy(rt); kws...)
