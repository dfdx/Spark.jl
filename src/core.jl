
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
    cmd = ser(identity)
    print(cmd)
    julia_rdd = JuliaRDD(rdd, cmd)
    jcall(julia_rdd.jrdd, "collect", JObject, ())    
end




## julia> convert(Vector{Uint32}, cmd)
## 12-element Array{Uint32,1}:
##  0x00000013
##  0x00000000
##  0x00000002
##  0x00000008
##  0x00000069
##  0x00000064
##  0x00000065
##  0x0000006e
##  0x00000074
##  0x00000069
##  0x00000074
##  0x00000079

## julia> convert(Vector{Int8}, cmd)
## 12-element Array{Int8,1}:
##   19
##    0
##    2
##    8
##  105
##  100
##  101
##  110
##  116
##  105
##  116
##  121

## julia> 
