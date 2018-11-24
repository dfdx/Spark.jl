
using JavaCall
import Base: map, reduce, count, collect, close
import Base.Iterators

# config
const JSparkConf = @jimport org.apache.spark.SparkConf
# context
const JSparkContext = @jimport org.apache.spark.SparkContext
const JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
# SQL
const JSparkSession = @jimport org.apache.spark.sql.SparkSession
const JStructType = @jimport org.apache.spark.sql.types.StructType
const JSparkSessionBuilder = @jimport org.apache.spark.sql.SparkSession$Builder
const JDataFrameReader = @jimport org.apache.spark.sql.DataFrameReader
const JDataFrameWriter = @jimport org.apache.spark.sql.DataFrameWriter
const JDataset = @jimport org.apache.spark.sql.Dataset
const JRelationalGroupedDataset = @jimport org.apache.spark.sql.RelationalGroupedDataset
const JGenericRow = @jimport org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
const JRowFactory = @jimport org.apache.spark.sql.RowFactory
const JRow = @jimport org.apache.spark.sql.Row
const JColumn = @jimport org.apache.spark.sql.Column
const JSQLFunctions = @jimport org.apache.spark.sql.functions
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

include("init.jl")
include("serialization.jl")
include("config.jl")
include("context.jl")
include("sql.jl")
include("rdd.jl")
include("attach.jl")
include("worker.jl")
