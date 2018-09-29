@testset "share_variable" begin

# tests the attach macro
a = 2

# test of basic funtionality
sc = SparkContext(master="local")

@share(sc, a)

txt = parallelize(sc, ["hello", "world"])
rdd = map(txt, it -> length(it) + a)

@test reduce(rdd, +) == 14

close(sc)

end
