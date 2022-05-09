
import Base: map, reduce, count, collect, close
import Base.Iterators

# config
const JSparkConf = @jimport org.apache.spark.SparkConf
# context
const JSparkContext = @jimport org.apache.spark.SparkContext
const JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext

# RDD
const JRDD = @jimport org.apache.spark.rdd.RDD
const JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
const JJavaPairRDD = @jimport org.apache.spark.api.java.JavaPairRDD
const JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD
const JJuliaPairRDD = @jimport org.apache.spark.api.julia.JuliaPairRDD
# utils
const JRDDUtils = @jimport org.apache.spark.api.julia.RDDUtils
# Java utils
const JIterator = @jimport java.util.Iterator
const JList = @jimport java.util.List
const JMap = @jimport java.util.Map
const JArrayList = @jimport java.util.ArrayList
const JHashMap = @jimport java.util.HashMap
const JSystem = @jimport java.lang.System


include("serialization.jl")
include("config.jl")
include("context.jl")
include("rdd.jl")
include("attach.jl")
include("worker.jl")
