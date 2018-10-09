@testset "reduce" begin

# test of reduce with no map stage
sc = SparkContext(master="local")
txt = parallelize(sc, ["hello", "world"])

@test reduce(txt, *) == "helloworld"

close(sc)

end
