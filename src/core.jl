
using JavaCall

JList = @jimport java.util.List
JArrays = @jimport java.util.Arrays
JJuliaRDD = @jimport org.apache.spark.api.julia.JuliaRDD
JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD
JJavaRDD_ = @jimport "org.apache.spark.api.java.JavaRDD\$"
JRDD = @jimport org.apache.spark.rdd.RDD
JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext


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



function ser(f::Function)
    buf = IOBuffer()
    serialize(buf, f)
    return buf.data
end

inc_it(it) = map(x -> x + 1, it)




function demo()
    sc = SparkContext()
    path = "file:///var/log/syslog"
    rdd = text_file(sc, path) # JavaRDD
    julia_rdd = JuliaRDD(rdd, ser(identity))
    r = jcall(julia_rdd.jrdd, "collect", JObject, ())
    listmethods(rdd.jrdd, "collect")
    listmethods(julia_rdd.jrdd, "collect")
    arr = convert(Array{Array{Uint8,1},1}, r)
end

