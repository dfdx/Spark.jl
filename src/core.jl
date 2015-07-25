
using JavaCall

JList = @jimport java.util.List
JArrays = @jimport java.util.Arrays
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJavaRDD_ = @jimport "org.apache.spark.api.java.JavaRDD\$"
JRDD = @jimport org.apache.spark.rdd.RDD
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext


# current task: work out Java-Julia protocol

function init()
    envcp = get(ENV, "CLASSPATH", "")
    # this should be sparta-assembly-*.jar
    spartajar = Pkg.dir("Sparta", "jvm", "sparta", "target", "sparta-0.1.jar")
    sparkjar = Pkg.dir("Sparta", "lib", "spark.jar")  
    classpath = "$envcp:$spartajar:$sparkjar"
    try
        # prevent exceptions in REPL
        JavaCall.init(["-ea", "-Xmx1024M", "-Djava.class.path=$classpath"])
    end
end

init()


include("context.jl")
include("rdd.jl")
include("worker.jl")



function demo()
    sc = SparkContext()
    path = "file:///var/log/syslog"
    rdd = text_file(sc, path) # JavaRDD    
    julia_rdd = JuliaRDD(rdd, jbyte[])
    jcall(julia_rdd.jrdd, "collect", JObject, ())
    listmethods(julia_rdd.jrdd, "collect")
end


