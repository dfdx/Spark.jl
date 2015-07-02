
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

