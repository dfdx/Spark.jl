@testset "reduce_by_key" begin

# test of group_by_key
sc = SparkContext(master="local")

nums1 = parallelize(sc, 1:3)
nums2 = parallelize(sc, 1:2)
rdd = cartesian(nums1, nums2)
rdd2 = reduce_by_key(rdd, +)
rdd3 = map(rdd2, it -> it[1] + it[2])
@test reduce(rdd3, +) == 15

close(sc)

end
