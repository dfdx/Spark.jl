@testset "map_partitions" begin

# test of map function

sc = SparkContext(master="local")

rdd = parallelize(sc, 1:10, n_split=2)
partitions = map_partitions(rdd, partition -> sum(partition))
values = collect(partitions)

@test values == [15, 40]

close(sc)

end
