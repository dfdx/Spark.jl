
## sql.jl - wrappers for Spark SQL / DataFrame / Dataset capabilities

## SparkSession

struct SparkSession
    jsess::JSparkSession
end

function SparkSession(;master="local",
                      appname="Julia App on Spark",
                      config=Dict{String, String}())
    jbuilder = jcall(JSparkSession, "builder", JSparkSessionBuilder, ())
    jcall(jbuilder, "master", JSparkSessionBuilder, (JString,), master)
    jcall(jbuilder, "appName", JSparkSessionBuilder, (JString,), appname)
    for (key, value) in config
        jcall(jbuilder, "config", JSparkSessionBuilder, (JString, JString), key, value)
    end
    jsess = jcall(jbuilder, "getOrCreate", JSparkSession, ())
    return SparkSession(jsess)
end

Base.show(io::IO, sess::SparkSession) = print(io, "SparkSession(...)")
Base.close(sess::SparkSession) = jcall(sess.jsess, "close", Void, ())


## Dataset

struct Dataset
    jds::JDataset
end


Base.show(io::IO, ds::Dataset) = jcall(ds.jds, "show", Void, ())


## IO formats

function dataframe_reader(sess::SparkSession)
    return jcall(sess.jsess, "read", JDataFrameReader, ())
end

function dataframe_writer(ds::Dataset)
    return jcall(ds.jds, "write", JDataFrameWriter, ())
end

# JSON

function read_json(sess::SparkSession, path::AbstractString)
    jreader = dataframe_reader(sess)
    jds = jcall(jreader, "json", JDataset, (JString,), path)
    return Dataset(jds)
end

function write_json(ds::Dataset, path::AbstractString)
    jwriter = dataframe_writer(ds)
    jcall(jwriter, "json", Void, (JString,), path)
end


# Parquet

function read_parquet(sess::SparkSession, path::AbstractString)
    jreader = dataframe_reader(sess)
    jds = jcall(jreader, "parquet", JDataset, (JString,), path)
    return Dataset(jds)
end

function write_parquet(ds::Dataset, path::AbstractString)
    jwriter = dataframe_writer(ds)
    jcall(jwriter, "parquet", Void, (JString,), path)
end


## main API

native_type(obj::JavaObject{Symbol("java.lang.Long")}) = jcall(obj, "longValue", jlong, ())
native_type(obj::JavaObject{Symbol("java.lang.Integer")}) = jcall(obj, "intValue", jint, ())
native_type(obj::JavaObject{Symbol("java.lang.Double")}) = jcall(obj, "doubleValue", jdouble, ())
native_type(obj::JavaObject{Symbol("java.lang.Float")}) = jcall(obj, "floatValue", jfloat, ())
native_type(obj::JavaObject{Symbol("java.lang.Boolean")}) = jcall(obj, "booleanValue", jboolean, ())
native_type(obj::JString) = unsafe_string(obj)
native_type(x) = x


Base.length(jrow::JGenericRow) = jcall(jrow, "length", jint, ())
# NOTE: getindex starts indexing from 1
Base.getindex(jrow::JGenericRow, i::Integer) = jcall(jrow, "get", JObject, (jint,), i-1)


function collect(ds::Dataset)
    jrows = jcall(ds.jds, "collectAsList", JList, ())
    data = Array{Any}(0)
    for jrow in JavaCall.iterator(jrows)
        arr = [native_type(narrow(jrow[i])) for i=1:length(jrow)]
        push!(data, (arr...))
    end
    return data
end




## temporary code


iterator(obj::JavaObject) = jcall(obj, "iterator", @jimport(java.util.Iterator), ())

"""
Given a `JavaObject{T}` narrows down `T` to a real class of the underlying object.
For example, `JavaObject{:java.lang.Object}` pointing to `java.lang.String`
will be narrowed down to `JavaObject{:java.lang.String}`
"""
function narrow(obj::JavaObject)
    c = jcall(obj, "getClass", @jimport(java.lang.Class), ())
    t = jcall(c, "getName", JString, ())
    return convert(JavaObject{Symbol(t)}, obj)
end
