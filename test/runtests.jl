using Spark

using Test

Spark.init()

@testset "Spark" begin

include("sql.jl")

# include("rdd/test_rdd.jl")

end
