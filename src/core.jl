
ENV["JAVA_LIB"] = "/usr/lib/jvm/java-7-oracle/jre/lib/amd64/server"

using JavaCall

JList = @jimport java.util.List
JArrays = @jimport java.util.Arrays
JJuliaRDD = @jimport sparta.JuliaRDD
# for some reason using Scala's version lead to segfault
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext


function init()
    envcp = get(ENV, "CLASSPATH", "")
    spartajar = Pkg.dir("Sparta", "jvm", "target", "scala-2.10",
                        "sparta_2.10-1.0.jar")
    sparkjar = Pkg.dir("Sparta", "lib", "spark.jar")
    classpath = "$envcp:$spartajar:$sparkjar"
    try
        # prevent exceptions in REPL
        JavaCall.init(["-ea", "-Xmx1024M", "-Djava.class.path=$classpath"])
    end
end

init()


type SparkContext
    jsc::JJavaSparkContext
end


function SparkContext(master::String="local", appname::String="This is Sparta!")
    SparkContext(JJavaSparkContext((JString, JString), master, appname))
end



abstract RDD

type JavaRDD <: RDD
    jrdd::JJavaRDD
end

type JuliaRDD <: RDD
    jrdd::JJuliaRDD
end


function text_file(sc::SparkContext, path::String)
    jrdd = jcall(sc.jsc, "textFile", JJavaRDD, (JString,), path)
    JavaRDD(jrdd)
end


## function Base.convert(::Type{JList}, A::Array)
##     jcall(JArrays, "asList", JList, (Vector{JObject},), A)
## end


## function parallelize(sc::SparkContext, collection)
##     jcoll = convert(JList, collect(collection))
##     jcall(sc.jsc, "parallelize", JJavaRDD, (JList,), jcoll)
##     JRDD(jcall(sc.jsc, "parallelize", JJavaRDD, (JList,), jcoll))
## end



function demo()
    sc = SparkContext()
    path = "file:///var/log/syslog"
    # jrdd = jcall(sc.jsc, "textFile", JJavaRDD, (JString,), path)
    # TODO: try JavaRDD instead
    
    # rdd = convert(JList, collect(map(jbox, collection)))
    rdd = text_file(sc, path)
end


function javacall_segfault()
    gurl = JavaCall.jnu((JString,), "http://www.google.com")
    jcall(gurl, "nonExistingMethod", jdouble, (jdouble,), 1.4)
end


## function text_file(sc::SparkContext, path::String)
##     JavaRDD(jcall(sc.jsc, "textFile", JRDD, (JString,), path))
## end


## # TODO: remove
## function test_text_file()
##     sc = SparkContext()
##     path = "/home/slipslop/texput.log"
##     rdd = text_file(sc, path)
## end
