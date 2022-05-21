using JavaCall
using Umlaut
import Umlaut.V
import Statistics
# using TableTraits
# using IteratorInterfaceExtensions


include("chainable.jl")
include("init.jl")
include("compiler.jl")
include("defs.jl")
include("convert.jl")
include("session.jl")
include("dataframe.jl")
include("column.jl")
include("row.jl")
include("struct.jl")
include("io.jl")
# include("sql.jl")
# include("streaming.jl")


function __init__()
    init()
end


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