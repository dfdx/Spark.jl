@testset "map_pair" begin

# test of map_pair
sc = SparkContext(master="local")

rdd = parallelize(sc, 1:3)
pairs = map_pair(rdd, x -> (x,1))
values = collect(pairs)

@test values == [(1,1), (2,1), (3,1)]

close(sc)

end
