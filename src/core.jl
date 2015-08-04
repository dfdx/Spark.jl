
using JavaCall


JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD

include("init.jl")
include("serialization.jl")
include("context.jl")
include("rdd.jl")
include("worker.jl")



function demo()
    sc = SparkContext()
    java_rdd = text_file(sc, "file:///var/log/syslog")
    rdd = PipelinedRDD(java_rdd, serialized(identity))
    r = jcall(rdd.jrdd, "collect", JObject, ())
    listmethods(java_rdd.jrdd, "collect")
    listmethods(rdd.jrdd, "collect")
    arr = convert(Array{Array{Uint8,1},1}, r)
end

