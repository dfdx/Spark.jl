@testset "julian_versions" begin

    # test filter
    sc = SparkContext(master="local")

    rdd = parallelize(sc, 1:30)
    pairs = map_pair(rdd, x -> (x,1))

    f = (prt, idx) -> sum(prt)
    @test (map_partitions_with_index(rdd, f) |> collect ==
           map_partitions_with_index(f, rdd) |> collect)

    f = prt -> sum(prt)
    @test (map_partitions(rdd, f) |> collect ==
           map_partitions(f, rdd) |> collect)

    f = prt -> ((x, 1) for x in prt)
    @test (map_partitions_pair(rdd, f) |> collect ==
           map_partitions_pair(f, rdd) |> collect)

    f = x -> x + 1
    @test map(rdd, f) |> collect == map(f, rdd) |> collect

    f = x -> (x,1)
    @test map_pair(rdd, f) |> collect == map_pair(f, rdd) |> collect

    f = x -> [x, x]
    @test flat_map(rdd, f) |> collect == flat_map(f, rdd) |> collect

    # currently broken
    # f -> x -> [x, x]
    # @test flat_map_pair(rdd, f) |> collect == flat_map_pair(f, rdd) |> collect

    f = x -> mod(x, 2) == 1
    @test filter(rdd, f) |> collect == filter(f, rdd) |> collect

    # currently broken
    # f = (x, y) -> mod(x, 2) == 1
    # @test filter(pairs, f) |> collect == filter(f, pairs) |> collect

    f = (+)
    @test reduce(rdd, f) |> collect == reduce(f, rdd) |> collect

    pairs2 = map_pair(parallelize(sc, ["a", "b", "a", "c"]), x -> (x, 1))
    f = (+)
    @test reduce_by_key(pairs2, f) |> collect == reduce_by_key(f, pairs2) |> collect

    close(sc)

end
