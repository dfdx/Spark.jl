module Spark

export SparkConfig,
       SparkContext,
       RDD,
       JuliaRDD,
       JavaRDD,
       text_file,
       parallelize,
       map,
       map_partitions,
       map_partitions_with_index,
       reduce,
       collect,
       count,
       close,
       typehint!,
       @attach,
       @share_variable

include("core.jl")

end 
