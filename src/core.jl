
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


include("context.jl")
include("rdd.jl")
include("worker.jl")



function demo()
    sc = SparkContext()
    path = "file:///var/log/syslog"
    rdd = text_file(sc, path)
end

