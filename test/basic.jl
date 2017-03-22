# test of basic funtionality
sc = SparkContext(master="local")
txt = parallelize(sc, ["hello", "world"])
rdd = map_partitions(txt, it -> map(s -> length(s), it))

@test count(rdd) == 2
@test reduce(rdd, +) == 10

close(sc)

