
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
    JavaRDD(jrdd)
end
