if Sys.isunix()
    ENV["JULIA_COPY_STACKS"] = 1
end

using Test
using Spark
import Statistics.mean

Spark.set_log_level("ERROR")

spark = Spark.SparkSession.builder.
    appName("Hello").
    master("local").
    config("some.key", "some-value").
    getOrCreate()


include("test_chainable.jl")
include("test_convert.jl")
include("test_compiler.jl")
include("test_sql.jl")
include("test_arrow.jl")

spark.stop()

# include("rdd/test_rdd.jl")

