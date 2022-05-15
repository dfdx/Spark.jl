using Test
using Spark

Spark.init()
Spark.set_log_level("ERROR")

include("test_chainable.jl")
include("test_compiler.jl")
include("test_sql.jl")
include("test_streaming.jl")


# include("rdd/test_rdd.jl")

