
## sql.jl - wrappers for Spark SQL / DataFrame / Dataset capabilities

using TableTraits
using IteratorInterfaceExtensions
using Arrow
using DataFrames

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
        jcall(jbuilder, "config", JSparkSessionBuilder, (JString, JString), key, string(value))
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


struct DatasetIterator{T}
    itr::JavaObject{Symbol("java.util.Iterator")}
    l::Int64
end

IteratorInterfaceExtensions.isiterable(x::Dataset) = true
TableTraits.isiterabletable(x::Dataset) = true

type_map = Dict(
    "LongType"    => Int64,
    "IntegerType" => Int32,
    "DoubleType"  => Float64,
    "FloatType"   => Float32,
    "BooleanType" => UInt8,
    "StringType"  => String,
    "ObjectType"  => JObject
)

function mapped_type(x::String) 
    if x in keys(type_map)
        return type_map[x]
    end

    return Any
end

function dtypes(ds::Dataset)
    jtypes = jcall(ds.jdf, "dtypes", Vector{JavaObject{Symbol("scala.Tuple2")}}, ())

    map(jtypes) do x
        name = convert(JString, jcall(x, "_1", JObject, ())) |> unsafe_string
        type = convert(JString, jcall(x, "_2", JObject, ())) |> unsafe_string
        (name=name, type=type)
    end
end


function TableTraits.getiterator(ds::Dataset)
    types = dtypes(ds)

    mnames = Symbol.(getfield.(types, :name))
    mtypes = mapped_type.(getfield.(types, :type))

    T = NamedTuple{Tuple(mnames),Tuple{mtypes...}}
    
    jit = jcall(ds.jdf, "toLocalIterator", JavaObject{Symbol("java.util.Iterator")}, ())

    l = count(ds)

    return DatasetIterator{T}(jit, l)
end

Base.IteratorSize(::Type{DatasetIterator{T}}) where {T} = Base.HasLength()
Base.length(x::DatasetIterator{T}) where {T} = x.l
Base.IteratorEltype(::Type{DatasetIterator{T}}) where {T} = T


function Base.eltype(iter::DatasetIterator{T}) where {T}
    return T
end

Base.eltype(::Type{DatasetIterator{T}}) where {T} = T


function Base.iterate(iter::DatasetIterator{T}, state=1) where {T}
    if Bool(jcall(iter.itr, "hasNext", jboolean, ()))
        jrow = convert(JRow, jcall(iter.itr, "next", JObject, ()))
        return as_named_tuple(Row(jrow)), state + 1
    end

    return nothing
end

function schema_ddl(ds::Dataset)
    jschema = jcall(ds.jdf, "schema", JStructType, ())
    jcall(jschema, "toDDL", JString, ())
end

function schema_string(ds::Dataset)
    jschema = jcall(ds.jdf, "schema", JStructType, ())
    jcall(jschema, "simpleString", JString, ())
end
show(ds::Dataset; numRows:: Int=20, truncate:: Bool=true) =
    jcall(ds.jdf, "show", Nothing, (jint, jboolean), numRows, truncate)
Base.show(io::IO, ds::Dataset) = print(io, "Dataset($(schema_string(ds)))")

function Base.names(ds::Dataset)
    jschema = jcall(ds.jdf, "schema", Spark.JStructType, ())
    jnames = jcall(jschema, "fieldNames", Array{JavaObject{Symbol("java.lang.String")},1}, ())

    names = unsafe_string.(jnames)

    return names
end


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

function read_df(sess::SparkSession, path::AbstractString="";
    format=nothing,
    schema=nothing,
    options=Dict()
)
    jreader = dataframe_reader(sess)
    if format != nothing
        jreader = jcall(jreader, "format", JDataFrameReader, (JString,), string(format))
    end
    if schema != nothing
        jreader = jcall(jreader, "schema", JDataFrameReader, (JString,), string(schema))
    end
    for (k, v) in options
        jreader = jcall(jreader, "option", JDataFrameReader, (JString, JString), string(k), string(v))
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


function Base.names(row::Row)
    jschema = jcall(row.jrow, "schema", Spark.JStructType, ())
    jnames = jcall(jschema, "fieldNames", Array{JavaObject{Symbol("java.lang.String")},1}, ())

    names = unsafe_string.(jnames)

    return names
end


function as_named_tuple(row, colnames)
    values = [native_type(narrow(row.jrow[i])) for i = 1:length(row.jrow)]
    return (; zip(colnames, values)...,)
end

function as_named_tuple(row)
    colnames = Symbol.(Base.names(row))
    return as_named_tuple(row, colnames)
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

Base.length(jrow::JRow) = jcall(jrow, "length", jint, ())
# NOTE: getindex starts indexing from 1
Base.getindex(jrow::JRow, i::Integer) = jcall(jrow, "get", JObject, (jint,), i - 1)

function cache(ds::Dataset)
    jds = jcall(ds.jdf, "cache", JDataset, ())
    return Dataset(jds)
end

"Returns the content of the Dataset as a Dataset of JSON strings."
function to_json(ds::Dataset)
    jds = jcall(ds.jdf, "toJSON", JDataset, ())
    Dataset(jds)
end

function checkpoint(ds::Dataset; eager::Bool = true)
    jds = jcall(ds.jdf, "checkpoint", JDataset, (jboolean,), eager)
    Dataset(jds)
end
function local_checkpoint(ds::Dataset; eager::Bool = true)
    jds = jcall(ds.jdf, "localCheckpoint", JDataset, (jboolean,), eager)
    Dataset(jds)
end

function collect(ds::Dataset)
    collect_to_arrow(ds) |> Tables.rowtable
end

"""
Returns an array of named tuples that contains all rows in this Dataset.

```
julia> sql(sess, "select 1 as a, 'x' as b, array(1, 2, 3) as c") |> collect_to_tuples
1-element Vector{NamedTuple{(:a, :b, :c), Tuple{Int32, String, Vector{Int32}}}}:
 (a = 1, b = "x", c = [1, 2, 3])
```
"""
function collect_to_tuples(ds::Dataset)
    collect_to_arrow(ds) |> Tables.rowtable
end

"""Returns a DataFrame that contains all rows in this Dataset."""
function collect_to_dataframe(ds::Dataset)
    collect_to_arrow(ds) |> DataFrame
end

"""Returns an Arrow.Table that contains all rows in this Dataset."""
function collect_to_arrow(ds::Dataset)
    arrowBytes = jcall(JDatasetUtils, "collectToArrow", Vector{jbyte}, (JDataset,), ds.jdf)
    arrowBytes = reinterpret(Vector{UInt8}, arrowBytes)
    Arrow.Table(arrowBytes)
end

"Returns the logical plans of the Dataset. Mode may be simple, extended, codegen, cost, or formatted"
function explain_string(ds::Dataset, mode::AbstractString="simple")
    jcall(JDatasetUtils, "explain", JString, (JDataset, JString,), ds.jdf, mode)
end
"Prints the logical plans to console of the Dataset. Mode may be simple, extended, codegen, cost, or formatted"
function explain(ds::Dataset, mode::AbstractString="simple")
    println(explain_string(ds, mode))
end

"""
Returns the number of rows in the Dataset.
```
julia> Spark.range(sess, 0, 100) |> count
100
```
"""
function count(ds::Dataset)
    return jcall(ds.jdf, "count", jlong, ())
end


"""
Returns a new Dataset by taking the first n rows.
```
julia> Spark.range(sess, 0, 888) |> Spark.limit(20) |> count
20
```
"""
function limit(ds::Dataset, count::Int32)
    jds = jcall(ds.jdf, "limit", JDataset, (jint,), count)
    Dataset(jds)
end
limit(count::Int32) = ds -> limit(ds, count)


"""
Creates a Spark Dataset from a SQL query.

```
julia> create_temp_view(Spark.range(sess, 0, 100), "my_table")
julia> sql(sess, "select id * 20 as a from my_table where id > 5 limit 5") |> collect_to_dataframe
5×1 DataFrame
 Row │ a     
     │ Int64 
─────┼───────
   1 │   120
   2 │   140
   3 │   160
   4 │   180
   5 │   200
```
"""
function sql(sess::SparkSession, str::AbstractString)
    jds = jcall(sess.jsess, "sql", JDataset, (JString,), str)
    return Dataset(jds)
end

"""
Creates a Dataset with a single LongType column named id, containing elements in a range from start to stop (exclusive)

```
julia> Spark.range(sess, 0, 100) |> Spark.count
100
```
"""
function range(sess::SparkSession, start::Int, stop::Int; step::Int=1, num_partitions::Union{Integer,Nothing}=nothing)
    if num_partitions === nothing
        Dataset(jcall(sess.jsess, "range", JDataset, (jlong,jlong,jlong), start, stop, step))
    else
        Dataset(jcall(sess.jsess, "range", JDataset, (jlong,jlong,jlong,jlong), start, stop, step, num_partitions))
    end
end

function table(sess::SparkSession, name::AbstractString)
    Dataset(jcall(sess.jsess, "table", JDataset, (JString,), name))
end

"""
Create Spark DataFrame from a Table (for example from DataFrames.jl)

```jl
julia> create_df(sess, DataFrame(a=[1, 2], b=["a", "b"]))
Dataset(struct<a:bigint,b:string>)

julia> DataFrame(a=[1, 2], b=["a", "b"]) |> create_df(sess)
Dataset(struct<a:bigint,b:string>)
```
"""
function create_df(sess::SparkSession, table)
    buff = IOBuffer()
    Arrow.write(buff, table)
    buffReintr = reinterpret(Vector{jbyte}, buff.data)
    Dataset(jcall(JDatasetUtils, "fromArrow", JDataset, (JSparkSession,Vector{jbyte}), sess.jsess, buffReintr))
end

create_df(sess::SparkSession) = table -> create_df(sess, table)

"""
```jl
julia> create_temp_view(Spark.range(sess, 0, 100), "a")

julia> sql(sess, "select * from a limit 3") |> collect_to_dataframe
3×1 DataFrame
 Row │ id    
     │ Int64 
─────┼───────
   1 │     0
   2 │     1
   3 │     2
```

julia> Spark.range(sess, 0, 100) |> create_temp_view("a")
"""
create_temp_view(ds::Dataset, str::AbstractString) =
    jcall(ds.jdf, "createTempView", Nothing, (JString,), str)
create_temp_view(str::AbstractString) =
    ds -> create_temp_view(ds, str)

"""
```jl
julia> create_temp_view(Spark.range(sess, 0, 100), "a")

julia> Spark.range(sess, 0, 1000) |> create_or_replace_temp_view("a")

julia> sql(sess, "select * from a where id % 3 == 0") |> Spark.count
334
"""
create_or_replace_temp_view(ds::Dataset, str::AbstractString) =
    jcall(ds.jdf, "createOrReplaceTempView", Nothing, (JString,), str)
create_or_replace_temp_view(str::AbstractString) =
    ds -> create_or_replace_temp_view(ds, str)


col(name::Union{String,Symbol}) =
    jcall(JSQLFunctions, "col", JColumn, (JString,), string(name))


function describe(ds::Dataset, col_names::Union{Symbol,String}...)
    col_names = [string(name) for name in col_names]
    jdf = jcall(ds.jdf, "describe", JDataset, (Vector{JString},), col_names[1:end])
    return Dataset(jdf)
end

"""
Returns the first row as a named tuple.
"""
function head(ds::Dataset)
    row = collect_to_tuples(limit(ds, 1))
    if isempty(row)
        error("Dataset $ds is empty")
    else
        row[1]
end


function select(df::Dataset, col_names::Union{Symbol,String}...)
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
