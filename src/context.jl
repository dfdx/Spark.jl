
"Wrapper around JavaSparkContext"
type SparkContext
    jsc::JJavaSparkContext
    appname::AbstractString
    tempdir::AbstractString
end


"""
Params:
 * master - address of application master. Currently only local and standalone modes
            are supported. Default is 'local'
 * appname - name of application
"""
function SparkContext(;master::AbstractString="local",
                      appname::AbstractString="Julia App on Spark")
    conf = SparkConf()
    setmaster(conf, master)
    setappname(conf, appname)
    jsc = JJavaSparkContext((JSparkConf,), conf.jconf)
    sc = SparkContext(jsc, appname, "")
    add_jar(sc, joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1.jar"))
    finalizer(sc, clear_up_tempdir)
    return sc
end

function SparkContext(jsc::JJavaSparkContext)
    appname = jcall(jsc, "appName", JString, ())
    sc = SparkContext(jsc, appname, "")
    finalizer(sc, clear_up_tempdir)
    return sc
end


function clear_up_tempdir(sc::SparkContext)
    if sc.tempdir != ""
        rm(sc.tempdir, recursive=true)
    end
end

function get_temp_dir(sc::SparkContext)
    if sc.tempdir == ""
        sc.tempdir = mktempdir()
    end
    return sc.tempdir
end

function Base.show(io::IO, sc::SparkContext)
    print(io, "SparkContext($(sc.appname))")
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
    java_rdd = JavaRDD(jrdd, Dict{Symbol,Any}(:styp => String,
                                              :typ => String))
    # turns out JavaRDD doesn't contain some methods, so we immediately wrap it
    # into a JuliaRDD/PipelinedRDD
    return PipelinedRDD(java_rdd, (idx, it) -> it, String)
end


function default_parallelism(sc::SparkContext)
    scala_sc = jcall(sc.jsc, "sc", JSparkContext, ())
    return jcall(scala_sc, "defaultParallelism", jint, ())
end


function parallelize(sc::SparkContext, coll; n_split::Int=-1)
    n_split = n_split == -1 ? default_parallelism(sc) : n_split
    tmp_path, f = mktemp()
    dump_stream(f, coll)
    close(f)
    jrdd = jcall(JJuliaRDD, "readRDDFromFile", JJavaRDD,
                 (JJavaSparkContext, JString, jint),
                 sc.jsc, tmp_path, n_split)
    java_rdd = JavaRDD(jrdd, Dict{Symbol,Any}(:styp => eltype(coll)))
    rdd = PipelinedRDD(java_rdd, (idx, it) -> it, eltype(coll))
    rm(tmp_path)
    return rdd
end
