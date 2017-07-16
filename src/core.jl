
using JavaCall
import Base: map, reduce, count, collect, close
import Iterators

# config
JSparkConf = @jimport org.apache.spark.SparkConf
# context
JSparkContext = @jimport org.apache.spark.SparkContext
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
# SQL
JSparkSession = @jimport org.apache.spark.sql.SparkSession
JSparkSessionBuilder = @jimport org.apache.spark.sql.SparkSession$Builder
JDataFrameReader = @jimport org.apache.spark.sql.DataFrameReader
JDataFrameWriter = @jimport org.apache.spark.sql.DataFrameWriter
JDataset = @jimport org.apache.spark.sql.Dataset
# JRow = @jimport org.apache.spark.sql.Row
JGenericRow = @jimport org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
# RDD
JRDD = @jimport org.apache.spark.rdd.RDD
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJavaPairRDD = @jimport org.apache.spark.api.java.JavaPairRDD
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD
JJuliaPairRDD = @jimport org.apache.spark.api.julia.JuliaPairRDD
# utils
JRDDUtils = @jimport org.apache.spark.api.julia.RDDUtils
# Java utils
JIterator = @jimport java.util.Iterator
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
