
using Docile
using JavaCall
using Iterators
import Base: map, reduce

JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD


include("init.jl")
include("serialization.jl")
include("decoders.jl")
include("context.jl")
include("rdd.jl")
include("worker.jl")


