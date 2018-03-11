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

### All of the following is to display rows in dict or JSON-like form

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
    rowdict(rt::RowTable, rowinds::AbstractVector)

Return the rows indexed by `rowinds` from `rt` wrapped so that they are pretty printed.
"""
function rowdict(rt::RowTable, rowinds::AbstractVector)
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

###

DataFrames.head(rt::RowTable, r::Int) = rt[1:min(r,size(rt,1)), :]
DataFrames.head(rt::RowTable) = DataFrames.head(rt, 6)
DataFrames.tail(rt::RowTable, r::Int) = rt[max(1,size(rt,1)-r+1):size(rt,1), :]
DataFrames.tail(rt::RowTable) = DataFrames.tail(rt, 6)
