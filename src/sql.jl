
## sql.jl - wrappers for Spark SQL / DataFrame / Dataset capabilities

## SparkSession

struct SparkSession
    jsess::JSparkSession
    master::AbstractString
    appname::AbstractString
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
    sess = SparkSession(jsess, master, appname)
    sc = context(sess)
    add_jar(sc, joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1.jar"))
    return sess
end

Base.show(io::IO, sess::SparkSession) = print(io, "SparkSession($(sess.master),$(sess.appname))")
Base.close(sess::SparkSession) = jcall(sess.jsess, "close", Nothing, ())

function context(sess::SparkSession)
    ssc = jcall(sess.jsess, "sparkContext", JSparkContext, ())
    jsc = jcall(JJavaSparkContext, "fromSparkContext",
                JJavaSparkContext, (JSparkContext,), ssc)
    return SparkContext(jsc)
end

## Dataset

struct Dataset
    jdf::JDataset
end


function schema_string(ds::Dataset)
    jschema = jcall(ds.jdf, "schema", JStructType, ())
    jcall(jschema, "simpleString", JString, ())
end
show(ds::Dataset) = jcall(ds.jdf, "show", Nothing, ())
Base.show(io::IO, ds::Dataset) = print(io, "Dataset($(schema_string(ds)))")


## IO formats

function dataframe_reader(sess::SparkSession)
    return jcall(sess.jsess, "read", JDataFrameReader, ())
end

function dataframe_writer(ds::Dataset)
    return jcall(ds.jdf, "write", JDataFrameWriter, ())
end

# JSON

function read_json(sess::SparkSession, path::AbstractString)
    jreader = dataframe_reader(sess)
    jds = jcall(jreader, "json", JDataset, (JString,), path)
    return Dataset(jds)
end

function write_json(ds::Dataset, path::AbstractString)
    jwriter = dataframe_writer(ds)
    jcall(jwriter, "json", Nothing, (JString,), path)
end


# Parquet

function read_parquet(sess::SparkSession, path::AbstractString)
    jreader = dataframe_reader(sess)
    jds = jcall(jreader, "parquet", JDataset, (JString,), path)
    return Dataset(jds)
end

function write_parquet(ds::Dataset, path::AbstractString)
    jwriter = dataframe_writer(ds)
    jcall(jwriter, "parquet", Nothing, (JString,), path)
end


# generic dataframe reader/writer

function read_df(sess::SparkSession, path::AbstractString=""; format=nothing, options=Dict())
    jreader = dataframe_reader(sess)
    if format != nothing
        jreader = jcall(jreader, "format", JDataFrameReader, (JString,), string(format))
    end
    for (k, v) in options
        jreader = jcall(jreader, "option", JDataFrameReader, (JString, JString), string(k), v)
    end
    jds = path != "" ?
        jcall(jreader, "load", JDataset, (JString,), path) :
        jcall(jreader, "load", JDataset, ())
    return Dataset(jds)
end


function write_df(ds::Dataset, path::AbstractString=""; format=nothing, mode=nothing, options=Dict())
    jwriter = dataframe_writer(ds)
    if format != nothing
        jwriter = jcall(jwriter, "format", JDataFrameWriter, (JString,), string(format))
    end
    if mode != nothing
        jwriter = jcall(jwriter, "mode", JDataFrameWriter, (JString,), string(mode))
    end
    for (k, v) in options
        jwriter = jcall(jwriter, "option", JDataFrameWriter, (JString, JString), string(k), v)
    end
    if path != ""
        jcall(jwriter, "save", Nothing, (JString,), path)
    else
        jcall(jwriter, "save", Nothing, (JString,))
    end
end


## Row

struct Row
    jrow::JRow
end

Row(objs...) = Row(jcall(JRowFactory, "create", JRow, (Vector{JObject},), [objs...]))


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
    jrows = jcall(ds.jdf, "collectAsList", JList, ())
    data = Array{Any}(nothing, 0)
    for jrow in JavaCall.iterator(jrows)
        arr = [native_type(narrow(jrow[i])) for i=1:length(jrow)]
        push!(data, (arr...,))
    end
    return data
end



function count(ds::Dataset)
    return jcall(ds.jdf, "count", jlong, ())
end


function sql(sess::SparkSession, str::AbstractString)
    jds = jcall(sess.jsess, "sql", JDataset, (JString,), str)
    return Dataset(jds)
end


col(name::Union{String, Symbol}) =
    jcall(JSQLFunctions, "col", JColumn, (JString,), string(name))



function select(df::Dataset, col_names::Union{Symbol, String}...)
    col_names = [string(name) for name in col_names]
    jdf = jcall(df.jdf, "select", JDataset, (JString, Vector{JString},), col_names[1], col_names[2:end])
    return Dataset(jdf)
end


## group by

struct RelationalGroupedDataset
    jrgd::JRelationalGroupedDataset
end


function group_by(ds::Dataset, col_names...)
    @assert length(col_names) > 0 "group_by requires at least one column name"
    jrgd = jcall(ds.jdf,"groupBy", JRelationalGroupedDataset,
                 (Vector{JColumn},), [col(col_name) for col_name in col_names])
    return RelationalGroupedDataset(jrgd)
end


function count(ds::RelationalGroupedDataset)
    return Dataset(jcall(ds.jrgd, "count", JDataset, ()))
end


## join

function join(left::Dataset, right::Dataset, col_name)
    jdf = jcall(left.jdf, "join", JDataset,
                (JDataset, JString), right.jdf, col_name)
    return Dataset(jdf)
end
