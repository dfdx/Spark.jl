
using Docile
using JavaCall
using Iterators

JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD


include("init.jl")
include("serialization.jl")
include("context.jl")
include("rdd.jl")
include("worker.jl")


# example function on partition iterator
@everywhere function take3(idx, it)
    println("Processing partition: $idx")
    return take(it, 3)
end

@everywhere function take4(it)
    return take(it, 4)
end

@everywhere function first5(arr)
    return arr[1:5]
end


function demo()
    sc = SparkContext()
    java_rdd = text_file(sc, "file:///var/log/syslog")
    rdd = map_partitions_with_index(java_rdd, take3)
    arr = collect(rdd)
    rdd = map_partitions(rdd, take4)
    arr = collect(rdd)
    rdd = map(rdd, first5)
    arr = collect(rdd)
end

