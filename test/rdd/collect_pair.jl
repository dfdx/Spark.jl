@testset "collect_pair" begin

# test of collect on pair rdds

sc = SparkContext(master="local")

nums1 = parallelize(sc, 1:2)
nums2 = parallelize(sc, 11:12)
pairs = cartesian(nums1, nums2)
values = collect(pairs)

@test values == [(1,11), (1,12), (2, 11), (2, 12)]

close(sc)

end
