using JavaCall
using Umlaut
import Umlaut.V
import Statistics
# using TableTraits
# using IteratorInterfaceExtensions

export SparkSession, DataFrame, GroupedData, Column, Row
export StructType, StructField, DataType
export Window, WindowSpec


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
include("window.jl")
include("io.jl")
include("streaming.jl")


function __init__()
    init()
end


# pseudo-modules for some specific functions not exported by default

module Compiler
    using Reexport
    @reexport import Spark: udf, jcall2, create_instance, create_class
end

# module SQL
#     using Reexport
#     @reexport import Spark: SparkSession, DataFrame, GroupedData, Column, Row
#     @reexport import Spark: StructType, StructField, DataType
#     @reexport import Spark: Window, WindowSpec
# end