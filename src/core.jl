
using JavaCall
import Base: map, reduce, count, collect, close
import Iterators

JSparkConf = @jimport org.apache.spark.SparkConf
JSparkContext = @jimport org.apache.spark.SparkContext
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
JSparkSession = @jimport org.apache.spark.sql.SparkSession
JRDD = @jimport org.apache.spark.rdd.RDD
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJavaPairRDD = @jimport org.apache.spark.api.java.JavaPairRDD
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD
JJuliaPairRDD = @jimport org.apache.spark.api.julia.JuliaPairRDD
JRDDUtils = @jimport org.apache.spark.api.julia.RDDUtils
JList = @jimport java.util.List
JMap = @jimport java.util.Map
JArrayList = @jimport java.util.ArrayList
JHashMap = @jimport java.util.HashMap

include("init.jl")
include("serialization.jl")
include("config.jl")
include("context.jl")
include("sql.jl")
include("flat_map_iterator.jl")
include("rdd.jl")
include("attach.jl")
include("worker.jl")
