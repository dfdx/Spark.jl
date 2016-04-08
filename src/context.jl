
"Wrapper around JavaSparkContext"
type SparkContext
    jsc::JJavaSparkContext
    master::AbstractString
    appname::AbstractString
end

"""
Params:
 * master - address of application master. Currently only local and standalone modes
            are supported. Default is 'local'
 * appname - name of application           
"""
function SparkContext(master::AbstractString="local",
                      appname::AbstractString="Julia App on Spark")
    jsc = JJavaSparkContext((JString, JString), master, appname)
    sc = SparkContext(jsc, master, appname)
    add_jar(sc, joinpath(Pkg.dir(), "Sparta", "jvm", "sparta", "target", "sparta-0.1.jar"))
    return sc
end

function Base.show(io::IO, sc::SparkContext)
    print(io, "SparkContext($(sc.master), $(sc.appname))")
end

"Close SparkContext"
function close(sc::SparkContext)
    jcall(sc.jsc, "close", Void, ())
end

"Add JAR file to SparkContext. Classes from this JAR will then be available to all tasks"
function add_jar(sc::SparkContext, path::AbstractString)
    jrdd = jcall(sc.jsc, "addJar", Void, (JString,), path)
end

"Add file to SparkContext. This file will be downloaded to each executor's work directory"
function add_file(sc::SparkContext, path::AbstractString)
    jrdd = jcall(sc.jsc, "addFile", Void, (JString,), path)
end

"Create RDD from a text file"
function text_file(sc::SparkContext, path::AbstractString)
    jrdd = jcall(sc.jsc, "textFile", JJavaRDD, (JString,), path)
    java_rdd = JavaRDD(jrdd, Dict{Symbol,Any}(:styp => UTF8String,
                                              :typ => UTF8String))
    # turns out JavaRDD doesn't contain some methods, so we immediately wrap it
    # into a JuliaRDD/PipelinedRDD
    return PipelinedRDD(java_rdd, (idx, it) -> it, UTF8String)
end
