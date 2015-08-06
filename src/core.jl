
using Docile
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
    rdd = PipelinedRDD(java_rdd, identity)
    arr = collect(rdd)
end
