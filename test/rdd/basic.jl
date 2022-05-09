@testset "basic" begin

# test of basic funtionality

#tests for config set/get
cnf = Spark.SparkConf()
cnf["abc"] = "def"
@test cnf["abc"] == "def"

#tests for SparkContext
sc = SparkContext(master="local")
txt = parallelize(sc, ["hello", "world"])
rdd = map_partitions(txt, it -> map(s -> length(s), it))

@test count(rdd) == 2
@test reduce(rdd, +) == 10

close(sc)

end
