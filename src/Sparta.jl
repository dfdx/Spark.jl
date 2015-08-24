module Sparta

export SparkContext,
       RDD,
       JuliaRDD,
       JavaRDD,
       text_file,
       map,
       map_partitions,
       map_partitions_with_index,
       reduce,
       collect,
       close

include("core.jl")

end # module
