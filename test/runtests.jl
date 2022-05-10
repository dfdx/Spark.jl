using Test
using Spark
import Spark: @chainable

Spark.init()

include("test_chainable.jl")
include("test_sql.jl")
include("test_streaming.jl")

#
# include("rdd/test_rdd.jl")

