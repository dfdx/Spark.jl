
using JavaCall


function init()    
    envcp = get(ENV, "CLASSPATH", "")
    confdir = Pkg.dir("Sparta", "conf")
    spartajar = Pkg.dir("Sparta", "lib", "sparta.jar")
    sparkjar = Pkg.dir("Sparta", "lib", "spark.jar")
    classpath = "$envcp:$confdir:$spartajar:$sparkjar"
    try
        # prevent exceptions in REPL
        JavaCall.init(["-ea", "-Xmx1024M", "-Djava.class.path=$classpath"])
    end
end

init()

JClass = @jimport java.lang.Class
JArrays = @jimport java.util.Arrays
JList = @jimport java.util.List
JFunction2 = @jimport org.apache.spark.api.java.function.Function2
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext


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



function text_file(sc::SparkContext, path::String)
    JavaRDD(jcall(sc.jsc, "textFile", JJavaRDD, (JString,), path))
end


# TODO: remove
function test_text_file()
    sc = SparkContext()
    path = "/home/slipslop/texput.log"
    rdd = text_file(sc, path)
end


