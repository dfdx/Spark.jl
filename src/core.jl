
using JavaCall

JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext


function init()
    envcp = get(ENV, "CLASSPATH", "")
    # TODO: provide uberjar for spark and sparta?
    spartajar = Pkg.dir("Sparta", "lib", "sparta.jar")
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


abstract RDD{T}

type JuliaRDD <: RDD
    jrdd::JJuliaRDD
end





function demo()
    sc = SparkContext()
    rdd = parallelize(sc, 1:10)
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
