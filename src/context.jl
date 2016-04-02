
type SparkContext
    jsc::JJavaSparkContext
end


function SparkContext(master::AbstractString="local",
                      appname::AbstractString="Julia App on Spark")
    SparkContext(JJavaSparkContext((JString, JString), master, appname))
end

function close(sc::SparkContext)
    jcall(sc.jsc, "close", Void, ())
end


function text_file(sc::SparkContext, path::AbstractString)
    jrdd = jcall(sc.jsc, "textFile", JJavaRDD, (JString,), path)
    java_rdd = JavaRDD(jrdd)
    # turns out JavaRDD doesn't contain some methods, so we immediately wrap it
    # into a JuliaRDD/PipelinedRDD
    return PipelinedRDD(java_rdd, (prt, it) -> it)  # TODO: this should work
    # return PipelinedRDD(java_rdd, identity)
    # return java_rdd
end
