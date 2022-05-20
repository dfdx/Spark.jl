const JSparkConf = @jimport org.apache.spark.SparkConf
const JRuntimeConfig = @jimport org.apache.spark.sql.RuntimeConfig
const JSparkContext = @jimport org.apache.spark.SparkContext
const JJavaSparkContext = @jimport org.apache.spark.api.java.JavaSparkContext
const JRDD = @jimport org.apache.spark.rdd.RDD
const JJavaRDD = @jimport org.apache.spark.api.java.JavaRDD

const JSparkSession = @jimport org.apache.spark.sql.SparkSession
const JSparkSessionBuilder = @jimport org.apache.spark.sql.SparkSession$Builder
const JDataFrameReader = @jimport org.apache.spark.sql.DataFrameReader
const JDataFrameWriter = @jimport org.apache.spark.sql.DataFrameWriter
const JDataset = @jimport org.apache.spark.sql.Dataset
# const JRelationalGroupedDataset = @jimport org.apache.spark.sql.RelationalGroupedDataset

# const JRowFactory = @jimport org.apache.spark.sql.RowFactory
const JGenericRow = @jimport org.apache.spark.sql.catalyst.expressions.GenericRow
const JGenericRowWithSchema = @jimport org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
const JRow = @jimport org.apache.spark.sql.Row
const JColumn = @jimport org.apache.spark.sql.Column
const JDataType = @jimport org.apache.spark.sql.types.DataType
const JMetadata = @jimport org.apache.spark.sql.types.Metadata
const JStructType = @jimport org.apache.spark.sql.types.StructType
const JStructField = @jimport org.apache.spark.sql.types.StructField
const JSQLFunctions = @jimport org.apache.spark.sql.functions

const JInteger = @jimport java.lang.Integer
const JLong = @jimport java.lang.Long
const JFloat = @jimport java.lang.Float
const JDouble = @jimport java.lang.Double
const JBoolean = @jimport java.lang.Boolean

const JList = @jimport java.util.List
const JArrayList = @jimport java.util.ArrayList



###############################################################################
#                                Type Definitions                             #
###############################################################################

struct SparkSessionBuilder
    jbuilder::JSparkSessionBuilder
end

struct SparkSession
    jspark::JSparkSession
end

struct RuntimeConfig
    jconf::JRuntimeConfig
end

struct DataFrame
    jdf::JDataset
end

struct DataFrameReader
    jreader::JDataFrameReader
end

struct DataFrameWriter
    jwriter::JDataFrameWriter
end

struct Column
    jcol::JColumn
end

struct Row
    jrow::JRow
end

struct StructType
    jst::JStructType
end

struct StructField
    jsf::JStructField
end


###############################################################################
#                                Conversions                                  #
###############################################################################

Base.convert(::Type{JObject}, x::Integer) = convert(JObject, convert(JLong, x))
Base.convert(::Type{JObject}, x::Real) = convert(JObject, convert(JDouble, x))
Base.convert(::Type{JObject}, x::Column) = convert(JObject, x.jcol)

Base.convert(::Type{Row}, obj::JObject) = Row(convert(JRow, obj))

Base.convert(::Type{String}, obj::JString) = unsafe_string(obj)
Base.convert(::Type{Integer}, obj::JLong) = jcall(obj, "longValue", jlong, ())

julia2java(::Type{String}) = JString
julia2java(::Type{Int64}) = JLong
julia2java(::Type{Int32}) = JInt
julia2java(::Type{Float64}) = JDouble
julia2java(::Type{Float32}) = JFloat
julia2java(::Type{Bool}) = JBoolean

java2julia(::Type{JString}) = String
java2julia(::Type{JLong}) = Int64
java2julia(::Type{JInteger}) = Int32
java2julia(::Type{JDouble}) = Float64
java2julia(::Type{JFloat}) = Float32
java2julia(::Type{JBoolean}) = Bool

julia2ddl(::Type{String}) = "string"
julia2ddl(::Type{Int64}) = "long"
julia2ddl(::Type{Int32}) = "int"
julia2ddl(::Type{Float64}) = "double"
julia2ddl(::Type{Float32}) = "float"
julia2ddl(::Type{Bool}) = "boolean"




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

for JT in (JString, JDouble, JLong, JBoolean)
    T = java2julia(JT)
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

# # build-time config
# function config(spark::SparkSession)
#     jctx = jcall(spark.jspark, "sparkContext", JSparkContext, ())
#     jconf = jcall(jctx, "getConf", JSparkConf, ())
#     entries = jcall(jconf, "getAll", Vector{@jimport scala.Tuple2}, ())
#     ret = Dict{String, Any}()
#     for e in entries
#         key = convert(JString, jcall(e, "_1", JObject, ())) |> unsafe_string
#         jval = jcall(e, "_2", JObject, ())
#         cls_name = getname(getclass(jval))
#         val = if cls_name == "java.lang.String"
#             unsafe_string(convert(JString, jval))
#         else
#             "(value type $cls_name is not supported)"
#         end
#         ret[key] = val
#     end
#     return ret
# end

# runtime config
function conf(spark::SparkSession)
    jconf = jcall(spark.jspark, "conf", JRuntimeConfig, ())
    return RuntimeConfig(jconf)
end


function createDataFrame(spark::SparkSession, rows::Vector{Row}, sch::StructType)
    if !isempty(rows)
        row = rows[1]
        if row.schema() != sch
            @warn "Schema mismatch:\n\trow     : $(row.schema())\n\tprovided: $sch"
        end
    end
    jrows = [row.jrow for row in rows]
    jrows_arr = convert(JArrayList, jrows)
    jdf = jcall(spark.jspark, "createDataFrame", JDataset, (JList, JStructType), jrows_arr, sch.jst)
    return DataFrame(jdf)
end

function createDataFrame(spark::SparkSession, rows::Vector{Row}, sch::String)
    st = StructType(sch)
    return spark.createDataFrame(rows, st)
end

function createDataFrame(spark::SparkSession, rows::Vector{Row})
    @assert !isempty(rows) "Cannot create a DataFrame from empty list of rows"
    st = rows[1].schema()
    return spark.createDataFrame(rows, st)
end


###############################################################################
#                                RuntimeConfig                                #
###############################################################################

# only supports set() and get(), most options should be set on builder instead
@chainable RuntimeConfig
Base.show(io::IO, cnf::RuntimeConfig) = print(io, "RuntimeConfig()")

Base.get(cnf::RuntimeConfig, name::String) =
    jcall(cnf.jconf, "get", JString, (JString,), name)
Base.get(cnf::RuntimeConfig, name::String, default::String) =
    jcall(cnf.jconf, "get", JString, (JString, JString), name, default)


function getAll(cnf::RuntimeConfig)
    jmap = jcall(cnf.jconf, "getAll", @jimport(scala.collection.immutable.Map), ())
    jiter = jcall(jmap, "iterator", @jimport(scala.collection.Iterator), ())
    ret = Dict{String, Any}()
    while Bool(jcall(jiter, "hasNext", jboolean, ()))
        jobj = jcall(jiter, "next", JObject, ())
        e = convert(@jimport(scala.Tuple2), jobj)
        key = convert(JString, jcall(e, "_1", JObject, ())) |> unsafe_string
        jval = jcall(e, "_2", JObject, ())
        cls_name = getname(getclass(jval))
        val = if cls_name == "java.lang.String"
            unsafe_string(convert(JString, jval))
        else
            "(value type $cls_name is not supported)"
        end
        ret[key] = val
    end
    return ret
end

for JT in (JString, JLong, JBoolean)
    T = java2julia(JT)
    @eval function set(cnf::RuntimeConfig, key::String, value::$T)
        jcall(cnf.jconf, "set", jvoid, (JString, $JT), key, value)
    end
end

###############################################################################
#                                  DataFrame                                  #
###############################################################################

Base.show(df::DataFrame) = jcall(df.jdf, "show", Nothing)
Base.show(io::IO, df::DataFrame) = show(df)
printSchema(df::DataFrame) = jcall(df.jdf, "printSchema", jvoid, ())


function Base.getindex(df::DataFrame, name::String)
    jcol = jcall(df.jdf, "col", JColumn, (JString,), name)
    return Column(jcol)
end

function Base.getproperty(df::DataFrame, prop::Symbol)
    if hasfield(DataFrame, prop)
        return getfield(df, prop)
    elseif string(prop) in columns(df)
        return df[string(prop)]
    else
        fn = getfield(@__MODULE__, prop)
        return DotChainer(df, fn)
    end
end

function columns(df::DataFrame)
    jnames = jcall(df.jdf, "columns", Vector{JString})
    names = [unsafe_string(jn) for jn in jnames]
    return names
end


Base.count(df::DataFrame) = jcall(df.jdf, "count", jlong)
Base.first(df::DataFrame) = Row(jcall(df.jdf, "first", JObject))

head(df::DataFrame) = Row(jcall(df.jdf, "head", JObject))
function  head(df::DataFrame, n::Integer)
    jobjs = jcall(df.jdf, "head", JObject, (jint,), n)
    jrows = convert(Vector{JRow}, jobjs)
    return map(Row, jrows)
end

function Base.collect(df::DataFrame)
    jobj = jcall(df.jdf, "collect", JObject, ())
    jrows = convert(Vector{JRow}, jobj)
    return map(Row, jrows)
end

take(df::DataFrame, n::Integer) =
    convert(Vector{Row}, jcall(df.jdf, "take", JObject, (jint,), n))


function select(df::DataFrame, cols::Column...)
    jdf = jcall(df.jdf, "select", JDataset, (Vector{JColumn},),
                [col.jcol for col in cols])
    return DataFrame(jdf)
end
select(df::DataFrame, cols::String...) = select(df, map(Column, cols)...)

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


for func in (:csv, :json, :parquet, :text, :textFile)
    @eval function $func(reader::DataFrameReader, paths::String...)
        jdf = jcall(reader.jreader, string($func), JDataset, (Vector{JString},), collect(paths))
        return DataFrame(jdf)
    end
end


function load(reader::DataFrameReader, paths::String...)
    # TODO: test with zero paths
    jdf = jcall(reader.jreader, "load", JDataset, (Vector{JString},), collect(paths))
    return DataFrame(jdf)
end


###############################################################################
#                                    Column                                   #
###############################################################################

function Column(name::String)
    jcol = jcall(JSQLFunctions, "col", JColumn, (JString,), name)
    return Column(jcol)
end

@chainable Column
function Base.show(io::IO, col::Column)
    name = jcall(col.jcol, "toString", JString)
    print(io, "col(\"$name\")")
end


# binary with JObject
for (func, name) in [(:+, "plus"), (:-, "minus"), (:*, "multiply"), (:/, "divide")]
    @eval function Base.$func(col::Column, obj::T) where T
        jres = jcall(col.jcol, $name, JColumn, (JObject,), obj)
        return Column(jres)
    end
end


alias(col::Column, name::String) =
    Column(jcall(col.jcol, "alias", JColumn, (JString,), name))

asc(col::Column) = Column(jcall(col.jcol, "asc", JColumn))
asc_nulls_first(col::Column) = Column(jcall(col.jcol, "asc_nulls_first", JColumn))
asc_nulls_last(col::Column) = Column(jcall(col.jcol, "asc_nulls_last", JColumn))

between(col::Column, low, up) =
    Column(jcall(col.jcol, "between", JColumn, (JObject, JObject), low, up))

bitwiseAND(col::Column, other) =
    Column(jcall(col.jcol, "bitwiseAND", JColumn, (JObject,), other))
Base.:&(col::Column, other) = bitwiseAND(col, other)

bitwiseOR(col::Column, other) =
    Column(jcall(col.jcol, "bitwiseOR", JColumn, (JObject,), other))
Base.:|(col::Column, other) = bitwiseOR(col, other)

bitwiseXOR(col::Column, other) =
    Column(jcall(col.jcol, "bitwiseXOR", JColumn, (JObject,), other))
Base.:⊻(col::Column, other) = bitwiseXOR(col, other)


Base.contains(col::Column, other) =
    Column(jcall(col.jcol, "contains", JColumn, (JObject,), other))

desc(col::Column) = Column(jcall(col.jcol, "desc", JColumn))
desc_nulls_first(col::Column) = Column(jcall(col.jcol, "desc_nulls_first", JColumn))
desc_nulls_last(col::Column) = Column(jcall(col.jcol, "desc_nulls_last", JColumn))

# dropFields should go here, but it's not in listmethods(col.jcol) ¯\_(ツ)_/¯

Base.endswith(col::Column, other) =
    Column(jcall2(col.jcol, "endsWith", JColumn, (JObject,), other))
Base.endswith(col::Column, other::Column) =
    Column(jcall(col.jcol, "endsWith", JColumn, (JColumn,), other.jcol))

eqNullSafe(col::Column, other) =
    Column(jcall(col.jcol, "eqNullSafe", JColumn, (JObject,), other))

Base.:(==)(col::Column, other) = Column(jcall(col.jcol, "equalTo", JColumn, (JObject,), other))
Base.:(!=)(col::Column, other) = Column(jcall(col.jcol, "notEqual", JColumn, (JObject,), other))

explain(col::Column, extended=false) = jcall(col.jcol, "explain", jvoid, (jboolean,), extended)

isNotNull(col::Column) = Column(jcall(col.jcol, "isNotNull", JColumn))
isNull(col::Column) = Column(jcall(col.jcol, "isNull", JColumn))

like(col::Column, s::String) = Column(jcall(col.jcol, "like", JColumn, (JString,), s))

otherwise(col::Column, other) =
    Column(jcall(col.jcol, "otherwise", JColumn, (JObject,), other))

over(col::Column) = Column(jcall(col.jcol, "over", JColumn))

rlike(col::Column, s::String) = Column(jcall(col.jcol, "rlike", JColumn, (JString,), s))

Base.startswith(col::Column, other) =
    Column(jcall2(col.jcol, "startsWith", JColumn, (JObject,), other))
Base.startswith(col::Column, other::Column) =
    Column(jcall(col.jcol, "startsWith", JColumn, (JColumn,), other.jcol))

substr(col::Column, start::Column, len::Column) =
    Column(jcall(col.jcol, "substr", JColumn, (JColumn, JColumn), start.jcol, len.jcol))
substr(col::Column, start::Integer, len::Integer) =
    Column(jcall(col.jcol, "substr", JColumn, (jint, jint), start, len))

when(col::Column, condition::Column, value) =
    Column(jcall(col.jcol, "when", JColumn, (JColumn, JObject), condition.jcol, value))


###############################################################################
#                                     Row                                     #
###############################################################################

function Row(; kv...)
    ks = map(string, keys(kv))
    vs = collect(values(values(kv)))
    flds = [StructField(k, julia2ddl(typeof(v)), true) for (k, v) in zip(ks, vs)]
    st = StructType(flds...)
    jrow = JGenericRowWithSchema((Vector{JObject}, JStructType,), vs, st.jst)
    jrow = convert(JRow, jrow)
    return Row(jrow)
end


function Base.show(io::IO, row::Row)
    str = jcall(row.jrow, "toString", JString)
    print(io, str)
end


function Base.getindex(row::Row, i::Integer)
    jobj = jcall(row.jrow, "get", JObject, (jint,), i - 1)
    class_name = getname(getclass(jobj))
    JT = JavaObject{Symbol(class_name)}
    T = java2julia(JT)
    return convert(T, convert(JT, jobj))
    # TODO: test all 4 types
end

function Base.getindex(row::Row, name::String)
    i = jcall(row.jrow, "fieldIndex", jint, (JString,), name)
    return row[i + 1]
end


schema(row::Row) = StructType(jcall(row.jrow, "schema", JStructType))


function Base.getproperty(row::Row, prop::Symbol)
    if hasfield(Row, prop)
        return getfield(row, prop)
    elseif string(prop) in names(schema(row))
        return row[string(prop)]
    else
        fn = getfield(@__MODULE__, prop)
        return DotChainer(row, fn)
    end
end


Base.:(==)(row1::Row, row2::Row) =
    Bool(jcall(row1.jrow, "equals", jboolean, (JObject,), row2.jrow))


###############################################################################
#                                  StructType                                 #
###############################################################################

StructType() = StructType(JStructType(()))

function StructType(flds::StructField...)
    st = StructType()
    for fld in flds
        st = add(st, fld)
    end
    return st
end

function StructType(sch::String)
    flds = StructField[]
    for name_ddl in split(sch, ",")
        name, ddl = split(strip(name_ddl), " ")
        push!(flds, StructField(name, ddl, true))
    end
    return StructType(flds...)
end

@chainable StructType
Base.show(io::IO, st::StructType) = print(io, jcall(st.jst, "toString", JString))

fieldNames(st::StructType) = convert(Vector{String}, jcall(st.jst, "fieldNames", Vector{JString}))
Base.names(st::StructType) = fieldNames(st)


add(st::StructType, sf::StructField) =
    StructType(jcall(st.jst, "add", JStructType, (JStructField,), sf.jsf))

Base.getindex(st::StructType, idx::Integer) =
    StructField(jcall(st.jst, "apply", JStructField, (jint,), idx - 1))

Base.getindex(st::StructType, name::String) =
    StructField(jcall(st.jst, "apply", JStructField, (JString,), name))


Base.:(==)(st1::StructType, st2::StructType) =
    Bool(jcall(st1.jst, "equals", jboolean, (JObject,), st2.jst))

###############################################################################
#                                  StructField                                #
###############################################################################

function StructField(name::AbstractString, typ::AbstractString, nullable::Bool)
    dtyp = jcall(JDataType, "fromDDL", JDataType, (JString,), typ)
    empty_metadata = jcall(JMetadata, "empty", JMetadata, ())
    jsf = jcall(
        JStructField, "apply", JStructField,
        (JString, JDataType, jboolean, JMetadata),
        name, dtyp, nullable, empty_metadata
    )
    return StructField(jsf)
end

Base.show(io::IO, sf::StructField) = print(io, jcall(sf.jsf, "toString", JString))

Base.:(==)(st1::StructField, st2::StructField) =
    Bool(jcall(st1.jsf, "equals", jboolean, (JObject,), st2.jsf))

# function Base.getproperty(row::Row, prop::Symbol)
#     if hasfield(Row, prop)
#         return getfield(row, prop)
#     elseif string(prop) in columns(df)  # requires schema
#         return df[string(prop)]
#     else
#         fn = getfield(@__MODULE__, prop)
#         return DotChainer(df, fn)
#     end
# end

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
