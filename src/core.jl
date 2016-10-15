
using JavaCall
using Iterators
import Base: map, reduce, count, collect, close

JSparkConf = @jimport org.apache.spark.SparkConf
JSparkContext = @jimport org.apache.spark.SparkContext
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD


include("init.jl")
include("serialization.jl")
include("config.jl")
include("context.jl")
include("rdd.jl")
include("attach.jl")
include("worker.jl")


