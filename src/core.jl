
using JavaCall
using Iterators
import Base: map, reduce, count, collect, close, filter

JSparkConf = @jimport org.apache.spark.SparkConf
JSparkContext = @jimport org.apache.spark.SparkContext
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
JRDD = @jimport org.apache.spark.rdd.RDD
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJavaPairRDD = @jimport org.apache.spark.api.java.JavaPairRDD
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD
JJuliaPairRDD = @jimport org.apache.spark.api.julia.JuliaPairRDD
JRDDUtils = @jimport org.apache.spark.api.julia.RDDUtils


include("init.jl")
include("serialization.jl")
include("config.jl")
include("context.jl")
include("flat_map_iterator.jl")
include("rdd.jl")
include("attach.jl")
include("worker.jl")


