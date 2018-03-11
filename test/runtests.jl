using Compat, Compat.Test
using RowTables
using DataStructures

@test size(RowTable()) == (0,0)

let
    a = [OrderedDict(:b => 3 , :a => 1),OrderedDict(:b => 3 , :a => 4.0),OrderedDict(:b => 3 , :a => 1)]
    b = [Dict(:a => 1 , :b => 3),Dict(:a => 4.0 , :b => 3),Dict(:a => 1 , :b => 3)]
    @test RowTable(a) == RowTable(b,[:b,:a])

    ds = [
        Dict(:a=>2,:b=>1,:c=>7)
        Dict(:a=>1,:b=>4,:c=>8)
        Dict(:a=>9,:b=>6,:c=>4)
        Dict(:a=>10,:b=>8,:c=>7)
        Dict(:a=>10,:b=>4,:c=>3)]

    # Construct from array of dictionaries
    rt = RowTable(ds,[:a,:b,:c])
    df = DataFrame(rt)
    @test rt == RowTable(df)

    rt1 = deepcopy(rt)
    @test rename!(rename!(rt1, [:a => :b, :b => :a]), [:a => :b, :b => :a]) == rt
    cyc = [:b => :a, :c => :b, :a => :c]
    @test rename!(rename!(rename!(rt1,cyc),cyc),cyc) == rt

    @test rt == rt[:]
    @test rt == rt[:,:]
    @test rt[:,1] == rt[1]
    @test rt[:,[1]] == rt[[1]]
    @test rt[1,1] == 2
    @test rt[1,:] == [2,1,7]
    @test rt[:,:b] == [1, 4, 6, 8, 4]
    @test rt[:,:b] == rt[:,2]
    @test rt[:,:a] == rt[:,1]
    # Construct from array of arrays.
    @test RowTable(rows(rt), [:a,:b,:c]) == rt

    rs = Any[[2, 7], [1, 8], [9, 4], [10, 7], [10, 3]]
    @test RowTable(rs,[:a,:c]) == rt[:,[:a,:c]]
    @test RowTable([[4, 8], [6, 4], [8, 7]], [:b,:c]) == rt[2:4,[:b,:c]]
    s = 0
    for r in rt
        s += sum(r)
    end
    @test s == 84

    @test pop!(copy(rt)) == [10,4,3]
    @test shift!(copy(rt)) == [2,1,7]

    df[:d] = ["b", "v", "d", "x", "x"]

    rt = RowTable(df)
    @test sort(rt) == RowTable(sort(df))
    @test sort(rt,cols=[2]) == RowTable(sort(df,cols=[2]))
    @test sort(rt,cols=[2],rev=true) == RowTable(sort(df,cols=[2],rev=true))
    @test sort(rt,cols=[2],rev=true) != RowTable(sort(df,cols=[2]))
    @test sort(rt,cols=[:d,:c],rev=true) == RowTable(sort(df,cols=[:d,:c],rev=true))

end
