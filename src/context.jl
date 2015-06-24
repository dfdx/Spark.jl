
type SparkContext
    jsc::JJavaSparkContext
end


function SparkContext(master::String="local", appname::String="This is Sparta!")
    SparkContext(JJavaSparkContext((JString, JString), master, appname))
end


function text_file(sc::SparkContext, path::String)
    jrdd = jcall(sc.jsc, "textFile", JJavaRDD, (JString,), path)
    JavaRDD(jrdd)
end


## function Base.convert(::Type{JList}, A::Array)
##     jcall(JArrays, "asList", JList, (Vector{JObject},), A)
## end


# TODO: no such method for convert
function parallelize(sc::SparkContext, collection)
    jcoll = convert(JList, collect(collection))
    jcall(sc.jsc, "parallelize", JJavaRDD, (JList,), jcoll)
    JavaRDD(jcall(sc.jsc, "parallelize", JJavaRDD, (JList,), jcoll))
end
