using JavaCall

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


include("dotcaller.jl")
include("init.jl")
include("compiler.jl")
include("sql.jl")

# mostly unsupported RDD interface
include("rdd/core.jl")


