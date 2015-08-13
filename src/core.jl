
using Docile
using JavaCall
using Iterators
import Base: map, reduce

JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD


include("init.jl")
include("serialization.jl")
include("context.jl")
include("rdd.jl")
include("worker.jl")


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


@everywhere function concat(a1, a2)
    # TODO: should take and produce iterator
    # TODO: finish it when at least strings are supported
    return [a1, a2]
end

function demo()
    sc = SparkContext()
    java_rdd = text_file(sc, "file:///var/log/syslog")
    rdd = map_partitions_with_index(java_rdd, take3)
    arr = collect(rdd)
    close(sc)
end

