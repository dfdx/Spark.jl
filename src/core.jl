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
include("rdd/core.jl")


