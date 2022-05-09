@testset "attach" begin

# tests the attach macro
@attach a = 2
@attach include("attach_include.jl")

# test of basic funtionality
sc = SparkContext(master="local")
txt = parallelize(sc, ["hello", "world"])
rdd = map_partitions(txt, it -> map(func_from_include, it))

@test reduce(rdd, +) == 14

close(sc)

end
