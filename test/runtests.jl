using Spark

using Test

Spark.init()

@testset "Spark" begin

include("basic.jl")
include("map.jl")
include("map_partitions.jl")
include("attach.jl")
include("reduce.jl")
include("text_file.jl")
include("share_variable.jl")
include("flat_map.jl")
include("cartesian.jl")
include("group_by_key.jl")
include("reduce_by_key.jl")
include("collect_pair.jl")
include("map_pair.jl")
include("julian_versions.jl")    
include("repartition_coalesce.jl")
include("filter.jl")
include("pipe.jl")
include("sql.jl")

end
