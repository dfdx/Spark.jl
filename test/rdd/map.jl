@testset "map" begin

# test of map function

sc = SparkContext(master="local")

rdd = parallelize(sc, 1:5)
mappedRdd = map(rdd, nr -> nr * 10)
values = collect(mappedRdd)

@test values == [10, 20, 30, 40, 50]

close(sc)

end
