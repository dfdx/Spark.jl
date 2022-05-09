@testset "repartition_coalesce" begin

# test repartition and coalesce
sc = SparkContext(master="local")

nums1 = parallelize(sc, 1:30)
@test num_partitions(nums1) == 1
nums3 = repartition(nums1, 3)
@test num_partitions(nums3) == 3
nums2 = Spark.coalesce(nums3, 2)
@test num_partitions(nums2) == 2
nums2 = Spark.coalesce(nums3, 2; shuffle=true)
@test num_partitions(nums2) == 2

pnums1 = cartesian(nums1, nums1)
@test num_partitions(pnums1) == 1
pnums3 = repartition(pnums1, 3)
@test num_partitions(pnums3) == 3
pnums2 = Spark.coalesce(pnums3, 2)
@test num_partitions(pnums2) == 2
pnums2 = Spark.coalesce(pnums3, 2; shuffle=true)
@test num_partitions(pnums2) == 2

close(sc)

end
