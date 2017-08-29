# test of map function

sc = SparkContext(master="local")

rdd = parallelize(sc, 1:10, n_split=2)
partitions = map_partitions(rdd, partition -> mean(partition))
values = collect(partitions)

@test values == [3, 8]

close(sc)

