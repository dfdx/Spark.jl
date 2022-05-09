@testset "flat_map" begin

# test of basic funtionality
sc = SparkContext(master="local")
nums = parallelize(sc, [1, 2, 3, 0, 4, 0])
rdd = flat_map(nums, it -> fill(it, it))

@test reduce(rdd, +) == 30

close(sc)

end
