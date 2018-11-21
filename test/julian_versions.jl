@testset "julian_versions" begin

# test filter
sc = SparkContext(master="local")

rdd = parallelize(sc, 1:30)

f = (partition, idx) -> sum(partition)
@test map_partitions_with_index(rdd, f) |> collect == map_partitions_with_index(f, rdd) |> collect
    @test map_partitions(rdd, partition -> sum(partition)) == map_partitions(partition -> sum(partition), rdd)
    @test map_partitions_pair(rdd, partition -> (sum(partition), 1)) == map_partitions_pair(rdd, partition -> (sum(partition), 1))
@test map_pair(rdd, x -> (x,1)) == map_pair(x -> (x,1), rdd)
    
    
# map_partitions_with_index(f::Function, rdd::RDD) = map_partition_with_index(rdd, f)
# map_partitions(f::Function, rdd::RDD) = map_partitions(rdd, f)
# map_partitions_pair(f::Function, rdd::RDD) = map_partitions_pair(rdd, f)
# Base.map(f::Function, rdd::RDD) = map(rdd, f)
# map_pair(f::Function, rdd::RDD) = map_pair(rdd, f)
# flat_map(f::Function, rdd::RDD) = flat_map(rdd, f)
# flat_map_pair(f::Function, rdd::RDD) = flat_map_pair(f, rdd)
# Base.filter(f::Function, rdd::SingleRDD) = filter(rdd, f)
# Base.filter(f::Function, rdd::PairRDD) = filter(rdd, f)
# Base.reduce(f::Function, rdd::RDD) = reduce(rdd, f)
#     reduce_by_key(f::Function, rdd::RDD) = reduce_by_key(rdd, f)

    
close(sc)

end
