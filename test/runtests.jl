using Spark
using Base.Test

include("basic.jl")
include("attach.jl")
include("reduce.jl")
include("text_file.jl")
include("share_variable.jl")
include("flat_map.jl")
include("cartesian.jl")
include("group_by_key.jl")
include("reduce_by_key.jl")
include("repartition_coalesce.jl")

