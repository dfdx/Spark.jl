module Spark

export SparkConfig,
       SparkContext,
       RDD,
       JuliaRDD,
       JavaRDD,
       text_file,
       parallelize,
       map,
       map_pair,
       map_partitions,
       map_partitions_pair,
       map_partitions_with_index,
       reduce,
       collect,
       count,
       close,
       typehint!,
       @attach,
       share_variable,
       @share,
       flat_map,
       flat_map_pair,
       cartesian,
       group_by_key,
       reduce_by_key

include("core.jl")

end 
