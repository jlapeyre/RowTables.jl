# Minimal implementation. Sort by columns

function _col_lt(icols...)
    function (x, y)
        @inbounds for c in icols
            x[c] == y[c] && continue
            return isless(x[c], y[c])
        end
        return false
    end
end

function Base.sort!(rt::RowTable; cols=[], kws...)
    if isempty(cols)
        icols = collect(1:size(rt, 2))
    else
        icols = cindex(rt)[cols]
    end
    nc = length(icols)
    if nc == 1
        @inbounds c = icols[1]
        Base.sort!(rows(rt); lt = (x, y)-> isless(@inbounds x[c], @inbounds y[c]), kws...)
    else
        Base.sort!(rows(rt); lt=_col_lt(icols...), kws...)
    end
    return rt
end

Base.sort(rt::RowTable; kws...) = sort!(copy(rt); kws...)
