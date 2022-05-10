const JSparkSession = @jimport org.apache.spark.sql.SparkSession
const JSparkSessionBuilder = @jimport org.apache.spark.sql.SparkSession$Builder
const JDataFrameReader = @jimport org.apache.spark.sql.DataFrameReader
const JDataFrameWriter = @jimport org.apache.spark.sql.DataFrameWriter
const JDataset = @jimport org.apache.spark.sql.Dataset
const JRelationalGroupedDataset = @jimport org.apache.spark.sql.RelationalGroupedDataset
const JGenericRow = @jimport org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
const JRowFactory = @jimport org.apache.spark.sql.RowFactory
const JRow = @jimport org.apache.spark.sql.Row
const JColumn = @jimport org.apache.spark.sql.Column
const JStructType = @jimport org.apache.spark.sql.types.StructType
const JSQLFunctions = @jimport org.apache.spark.sql.functions


###############################################################################
#                                Type Definitions                             #
###############################################################################

struct SparkSessionBuilder
    jbuilder::JSparkSessionBuilder
end

struct SparkSession
    jspark::JSparkSession
end

struct DataFrame
    jdf::JDataset
end

struct DataFrameReader
    jreader::JDataFrameReader
end


###############################################################################
#                            SparkSession.Builder                             #
###############################################################################

@chainable SparkSessionBuilder
Base.show(io::IO, ::SparkSessionBuilder) = print(io, "SparkSessionBuilder()")


function appName(builder::SparkSessionBuilder, name::String)
    jcall(builder.jbuilder, "appName", JSparkSessionBuilder, (JString,), name)
    return builder
end


function master(builder::SparkSessionBuilder, uri::String)
    jcall(builder.jbuilder, "master", JSparkSessionBuilder, (JString,), uri)
    return builder
end


for (T, JT) in [(String, JString), (Integer, jlong), (Real, jdouble), (Bool, jboolean)]
    @eval function config(builder::SparkSessionBuilder, key::String, value::$T)
        jcall(builder.jbuilder, "config", JSparkSessionBuilder, (JString, $JT), key, value)
        return builder
    end
end


function enableHiveSupport(builder::SparkSessionBuilder)
    jcall(builder.jbuilder, "enableHiveSupport", JSparkSessionBuilder)
    return builder
end


function getOrCreate(builder::SparkSessionBuilder)
    config(builder, "spark.jars", joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1.jar"))
    jspark = jcall(builder.jbuilder, "getOrCreate", JSparkSession, ())
    return SparkSession(jspark)
end


###############################################################################
#                                 SparkSession                                #
###############################################################################

@chainable SparkSession
Base.show(io::IO, ::SparkSession) = print(io, "SparkSession()")


function Base.getproperty(::Type{SparkSession}, prop::Symbol)
    if prop == :builder
        jbuilder = jcall(JSparkSession, "builder", JSparkSessionBuilder, ())
        return SparkSessionBuilder(jbuilder)
    else
        return getfield(SparkSession, prop)
    end
end

Base.close(spark::SparkSession) = jcall(spark.jspark, "close", Nothing, ())
stop(spark::SparkSession) = jcall(spark.jspark, "stop", Nothing, ())


function Base.read(spark::SparkSession)
    jreader = jcall(spark.jspark, "read", JDataFrameReader, ())
    return DataFrameReader(jreader)
end


###############################################################################
#                                  DataFrame                                  #
###############################################################################

@chainable DataFrame
Base.show(df::DataFrame) = jcall(df.jdf, "show", Nothing)
Base.show(io::IO, ::DataFrame) = show(df)


###############################################################################
#                                DataFrameReader                              #
###############################################################################

@chainable DataFrameReader
Base.show(io::IO, ::DataFrameReader) = print(io, "DataFrameReader()")


function format(reader::DataFrameReader, src::String)
    jcall(reader.jreader, "format", JDataFrameReader, (JString,), src)
    return reader
end


for (T, JT) in [(String, JString), (Integer, jlong), (Real, jdouble), (Bool, jboolean)]
    @eval function option(reader::DataFrameReader, key::String, value::$T)
        jcall(reader.jreader, "option", JDataFrameReader, (JString, $JT), key, value)
        return reader
    end
end


for func in (:csv, :json, :parquet, :text. :textFile)
    @eval function $func(reader::DataFrameReader, paths::String...)
        jdf = jcall(reader.jreader, string(func), JDataset, (Vector{JString},), collect(paths))
        return DataFrame(jdf)
    end
end


function load(reader::DataFrameReader, paths::String...)
    # TODO: test with zero paths
    jdf = jcall(reader.jreader, "load", JDataset, (Vector{JString},), collect(paths))
    return DataFrame(jdf)
end


# struct DatasetIterator{T}
#     itr::JavaObject{Symbol("java.util.Iterator")}
#     l::Int64
# end

# IteratorInterfaceExtensions.isiterable(x::Dataset) = true
# TableTraits.isiterabletable(x::Dataset) = true

# type_map = Dict(
#     "LongType"    => Int64,
#     "IntegerType" => Int32,
#     "DoubleType"  => Float64,
#     "FloatType"   => Float32,
#     "BooleanType" => UInt8,
#     "StringType"  => String,
#     "ObjectType"  => JObject
# )

# function mapped_type(x::String)
#     if x in keys(type_map)
#         return type_map[x]
#     end

#     return Any
# end


# function TableTraits.getiterator(ds::Dataset)
#     jtypes = jcall(ds.jdf, "dtypes", Vector{JavaObject{Symbol("scala.Tuple2")}}, ())

#     mnames = Symbol.(unsafe_string.(map(x -> convert(JString, jcall(x, "_1", JObject, ())), jtypes)))
#     mtypes = mapped_type.(unsafe_string.(map(x -> convert(JString, jcall(x, "_2", JObject, ())), jtypes)))

#     T = NamedTuple{Tuple(mnames),Tuple{mtypes...}}

#     jit = jcall(ds.jdf, "toLocalIterator", JavaObject{Symbol("java.util.Iterator")}, ())

#     l = count(ds)

#     return DatasetIterator{T}(jit, l)
# end

# Base.IteratorSize(::Type{DatasetIterator{T}}) where {T} = Base.HasLength()
# Base.length(x::DatasetIterator{T}) where {T} = x.l
# Base.IteratorEltype(::Type{DatasetIterator{T}}) where {T} = T


# function Base.eltype(iter::DatasetIterator{T}) where {T}
#     return T
# end

# Base.eltype(::Type{DatasetIterator{T}}) where {T} = T


# function Base.iterate(iter::DatasetIterator{T}, state=1) where {T}
#     if Bool(jcall(iter.itr, "hasNext", jboolean, ()))
#         jrow = convert(JRow, jcall(iter.itr, "next", JObject, ()))
#         return as_named_tuple(Row(jrow)), state + 1
#     end

#     return nothing
# end

# function schema_string(ds::Dataset)
#     jschema = jcall(ds.jdf, "schema", JStructType, ())
#     jcall(jschema, "simpleString", JString, ())
# end
# show(ds::Dataset) = jcall(ds.jdf, "show", Nothing, ())
# Base.show(io::IO, ds::Dataset) = print(io, "Dataset($(schema_string(ds)))")

# function Base.names(ds::Dataset)
#     jschema = jcall(ds.jdf, "schema", Spark.JStructType, ())
#     jnames = jcall(jschema, "fieldNames", Array{JavaObject{Symbol("java.lang.String")},1}, ())

#     names = unsafe_string.(jnames)

#     return names
# end


# ## IO formats

# function dataframe_reader(sess::SparkSession)
#     return jcall(sess.jsess, "read", JDataFrameReader, ())
# end

# function dataframe_writer(ds::Dataset)
#     return jcall(ds.jdf, "write", JDataFrameWriter, ())
# end

# # JSON

# function read_json(sess::SparkSession, path::AbstractString)
#     jreader = dataframe_reader(sess)
#     jds = jcall(jreader, "json", JDataset, (JString,), path)
#     return Dataset(jds)
# end

# function write_json(ds::Dataset, path::AbstractString)
#     jwriter = dataframe_writer(ds)
#     jcall(jwriter, "json", Nothing, (JString,), path)
# end


# # Parquet

# function read_parquet(sess::SparkSession, path::AbstractString)
#     jreader = dataframe_reader(sess)
#     jds = jcall(jreader, "parquet", JDataset, (JString,), path)
#     return Dataset(jds)
# end

# function write_parquet(ds::Dataset, path::AbstractString)
#     jwriter = dataframe_writer(ds)
#     jcall(jwriter, "parquet", Nothing, (JString,), path)
# end


# # generic dataframe reader/writer

# function read_df(sess::SparkSession, path::AbstractString=""; format=nothing, options=Dict())
#     jreader = dataframe_reader(sess)
#     if format != nothing
#         jreader = jcall(jreader, "format", JDataFrameReader, (JString,), string(format))
#     end
#     for (k, v) in options
#         jreader = jcall(jreader, "option", JDataFrameReader, (JString, JString), string(k), v)
#     end
#     jds = path != "" ?
#         jcall(jreader, "load", JDataset, (JString,), path) :
#         jcall(jreader, "load", JDataset, ())
#     return Dataset(jds)
# end


# function write_df(ds::Dataset, path::AbstractString=""; format=nothing, mode=nothing, options=Dict())
#     jwriter = dataframe_writer(ds)
#     if format != nothing
#         jwriter = jcall(jwriter, "format", JDataFrameWriter, (JString,), string(format))
#     end
#     if mode != nothing
#         jwriter = jcall(jwriter, "mode", JDataFrameWriter, (JString,), string(mode))
#     end
#     for (k, v) in options
#         jwriter = jcall(jwriter, "option", JDataFrameWriter, (JString, JString), string(k), v)
#     end
#     if path != ""
#         jcall(jwriter, "save", Nothing, (JString,), path)
#     else
#         jcall(jwriter, "save", Nothing, (JString,))
#     end
# end


# ## Row

# struct Row
#     jrow::JRow
# end

# @chainable Row

# Row(objs...) = Row(jcall(JRowFactory, "create", JRow, (Vector{JObject},), [objs...]))


# function Base.names(row::Row)
#     jschema = jcall(row.jrow, "schema", Spark.JStructType, ())
#     jnames = jcall(jschema, "fieldNames", Array{JavaObject{Symbol("java.lang.String")},1}, ())

#     names = unsafe_string.(jnames)

#     return names
# end


# function as_named_tuple(row, colnames)
#     values = [native_type(narrow(row.jrow[i])) for i = 1:length(row.jrow)]
#     return (; zip(colnames, values)...,)
# end

# function as_named_tuple(row)
#     colnames = Symbol.(Base.names(row))
#     return as_named_tuple(row, colnames)
# end


# ## main API

# native_type(obj::JavaObject{Symbol("java.lang.Long")}) = jcall(obj, "longValue", jlong, ())
# native_type(obj::JavaObject{Symbol("java.lang.Integer")}) = jcall(obj, "intValue", jint, ())
# native_type(obj::JavaObject{Symbol("java.lang.Double")}) = jcall(obj, "doubleValue", jdouble, ())
# native_type(obj::JavaObject{Symbol("java.lang.Float")}) = jcall(obj, "floatValue", jfloat, ())
# native_type(obj::JavaObject{Symbol("java.lang.Boolean")}) = jcall(obj, "booleanValue", jboolean, ())
# native_type(obj::JString) = unsafe_string(obj)
# native_type(x) = x


# Base.length(jrow::JGenericRow) = jcall(jrow, "length", jint, ())
# # NOTE: getindex starts indexing from 1
# Base.getindex(jrow::JGenericRow, i::Integer) = jcall(jrow, "get", JObject, (jint,), i-1)

# Base.length(jrow::JRow) = jcall(jrow, "length", jint, ())
# # NOTE: getindex starts indexing from 1
# Base.getindex(jrow::JRow, i::Integer) = jcall(jrow, "get", JObject, (jint,), i - 1)

# function cache(ds::Dataset)
#     jds = jcall(ds.jdf, "cache", JDataset, ())
#     return Dataset(jds)
# end

# function Base.collect(ds::Dataset)
#     jrows = jcall(ds.jdf, "collectAsList", JList, ())
#     data = Array{Any}(nothing, 0)
#     for jrow in JavaCall.iterator(jrows)
#         arr = [native_type(narrow(jrow[i])) for i=1:length(jrow)]
#         push!(data, (arr...,))
#     end
#     return data
# end



# function Base.count(ds::Dataset)
#     return jcall(ds.jdf, "count", jlong, ())
# end


# function sql(sess::SparkSession, str::AbstractString)
#     jds = jcall(sess.jsess, "sql", JDataset, (JString,), str)
#     return Dataset(jds)
# end


# create_temp_view(ds::Dataset, str::AbstractString) =
#     jcall(ds.jdf, "createTempView", Nothing, (JString,), str)


# create_or_replace_temp_view(ds::Dataset, str::AbstractString) =
#     jcall(ds.jdf, "createOrReplaceTempView", Nothing, (JString,), str)


# col(name::Union{String,Symbol}) =
#     jcall(JSQLFunctions, "col", JColumn, (JString,), string(name))


# function describe(ds::Dataset, col_names::Union{Symbol,String}...)
#     col_names = [string(name) for name in col_names]
#     jdf = jcall(ds.jdf, "describe", JDataset, (Vector{JString},), col_names[1:end])
#     return Dataset(jdf)
# end

# function head(ds::Dataset)
#     jrow = convert(JRow, jcall(ds.jdf, "head", JObject, ()))
#     return Row(jrow)
# end


# function select(df::Dataset, col_names::Union{Symbol,String}...)
#     col_names = [string(name) for name in col_names]
#     jdf = jcall(df.jdf, "select", JDataset, (JString, Vector{JString},), col_names[1], col_names[2:end])
#     return Dataset(jdf)
# end


# ## group by

# struct RelationalGroupedDataset
#     jrgd::JRelationalGroupedDataset
# end

# @chainable RelationalGroupedDataset

# function group_by(ds::Dataset, col_names...)
#     @assert length(col_names) > 0 "group_by requires at least one column name"
#     jrgd = jcall(ds.jdf,"groupBy", JRelationalGroupedDataset,
#                  (Vector{JColumn},), [col(col_name) for col_name in col_names])
#     return RelationalGroupedDataset(jrgd)
# end


# function Base.count(ds::RelationalGroupedDataset)
#     return Dataset(jcall(ds.jrgd, "count", JDataset, ()))
# end


# ## join

# function join(left::Dataset, right::Dataset, col_name)
#     jdf = jcall(left.jdf, "join", JDataset,
#                 (JDataset, JString), right.jdf, col_name)
#     return Dataset(jdf)
# end
