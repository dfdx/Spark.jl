ENV["JULIA_COPY_STACKS"] = 1

using Test
using Spark


Spark.init()
Spark.set_log_level("ERROR")

spark = Spark.SparkSession.builder.
    appName("Hello").
    master("local").
    config("some.key", "some-value").
    getOrCreate()


include("test_chainable.jl")
include("test_compiler.jl")
include("test_sql.jl")
include("test_streaming.jl")

spark.stop()

# include("rdd/test_rdd.jl")

