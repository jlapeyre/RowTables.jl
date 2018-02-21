## Something quick and dirty for now.

# function Base.sort!(rt::RowTable; alg=nothing,
#                     lt=isless, by=identity, rev=false, order=Forward)
#     Base.sort!(rows(rt),alg=alg,lt=lt,by=by,rev=rev,order=order)
#     return rt
# end

function Base.sort!(rt::RowTable; kws...)
    Base.sort!(rows(rt);kws...)
    return rt
end

