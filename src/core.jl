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

# mostly unsupported RDD interface
# include("rdd/core.jl")


# During development we use just include("core.jl") and get a single
# namespace with all the functions. But for the outer world we also
# provide a set of modules mimicing PySpark package layout

module Compiler
    import ..jcall2
    export jcall2

    import ..udf
    export udf
end

module SQL
    using Reexport
    @reexport import Spark: SparkSession, DataFrame, Column, Row
    @reexport import Spark: config
    # import ..SparkSession
    # export SparkSession

    # import ..DataFrame
    # export DataFrame

    # import ..Column
    # export Column
end