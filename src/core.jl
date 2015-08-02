
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

using Compat
import Base.convert
import JavaCall: JNIEnv, jnifunc, penv

## function convert(::Type{Array{JObject, 1}}, obj::JObject)
##     sz = ccall(jnifunc.GetArrayLength, jint,
##                (Ptr{JNIEnv}, Ptr{Void}), penv, obj.ptr)
##     ret = Array(JObject, sz)
##     for i=1:sz
##         ptr = ccall(jnifunc.GetObjectArrayElement, Ptr{Void},
##                   (Ptr{JNIEnv}, Ptr{Void}, jint), penv, obj.ptr, i-1)
##         ret[i] = JObject(ptr)
##     end
##     return ret
## end

for (x, y, z) in [ (:jboolean, :(jnifunc.GetBooleanArrayElements), :(jnifunc.ReleaseBooleanArrayElements)),
                  (:jchar, :(jnifunc.GetCharArrayElements), :(jnifunc.ReleaseCharArrayElements)),
                  (:jbyte, :(jnifunc.GetByteArrayElements), :(jnifunc.ReleaseByteArrayElements)),
                  (:jshort, :(jnifunc.GetShortArrayElements), :(jnifunc.ReleaseShortArrayElements)),
                  (:jint, :(jnifunc.GetIntArrayElements), :(jnifunc.ReleaseIntArrayElements)),
                  (:jlong, :(jnifunc.GetLongArrayElements), :(jnifunc.ReleaseLongArrayElements)),
                  (:jfloat, :(jnifunc.GetFloatArrayElements), :(jnifunc.ReleaseFloatArrayElements)),
                  (:jdouble, :(jnifunc.GetDoubleArrayElements), :(jnifunc.ReleaseDoubleArrayElements)) ]
    m=quote
        function convert(::Type{Array{$(x),1}}, obj::JObject)
            sz = ccall(jnifunc.GetArrayLength, jint, (Ptr{JNIEnv}, Ptr{Void}), penv, obj.ptr)    
            arr = ccall($(y), Ptr{$(x)}, (Ptr{JNIEnv}, Ptr{Void}, Ptr{jboolean} ), penv, obj.ptr, C_NULL )
            jl_arr::Array = pointer_to_array(arr, (@compat Int(sz)), false)
            jl_arr = deepcopy(jl_arr)
            ccall($(z), Void, (Ptr{JNIEnv},Ptr{Void}, Ptr{$(x)}, jint), penv, obj.ptr, arr, 0)
            return jl_arr
        end
    end
    eval(m)
end


function convert{T}(::Type{Array{T, 1}}, obj::JObject)
    sz = ccall(jnifunc.GetArrayLength, jint,
               (Ptr{JNIEnv}, Ptr{Void}), penv, obj.ptr)
    ret = Array(T, sz)
    for i=1:sz
        ptr = ccall(jnifunc.GetObjectArrayElement, Ptr{Void},
                  (Ptr{JNIEnv}, Ptr{Void}, jint), penv, obj.ptr, i-1)
        ret[i] = convert(T, JObject(ptr))
    end
    return ret
end



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

