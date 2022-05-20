using JavaCall
using Umlaut
import Umlaut.V
# using TableTraits
# using IteratorInterfaceExtensions


include("chainable.jl")
include("init.jl")
include("compiler.jl")
include("sql.jl")
include("streaming.jl")


function __init__()
    init()
end

# mostly unsupported RDD interface
# include("rdd/core.jl")


# During development we use just include("core.jl") and get a single
# namespace with all the functions. But for the outer world we also
# provide a set of modules mimicing PySpark package layout

module Compiler
    using Reexport
    @reexport import Spark: udf, jcall2
end

module SQL
    using Reexport
    @reexport import Spark: SparkSession, DataFrame, GroupedData, Column, Row
    @reexport import Spark: StructType, StructField, DataType
    @reexport import Spark: config
end