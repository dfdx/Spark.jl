###############################################################################
#                            SparkSession.Builder                             #
###############################################################################

import Tables
import TableTraits
import DataFrames
import Arrow

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
    jcall(builder.jbuilder, "enableHiveSupport", JSparkSessionBuilder, ())
    return builder
end

function getOrCreate(builder::SparkSessionBuilder)
    config(builder, "spark.jars", joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.2.jar"))
    jspark = jcall(builder.jbuilder, "getOrCreate", JSparkSession, ())
    return SparkSession(jspark)
end


###############################################################################
#                                 SparkSession                                #
###############################################################################
Base.propertynames(::SparkSession, private::Bool=false) = [:version, :stop, :conf, :createDataFrame, :createDataFrameFromTable, :sql]
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


function read(spark::SparkSession)
    jreader = jcall(spark.jspark, "read", JDataFrameReader, ())
    return DataFrameReader(jreader)
end

# note: write() method is defined in dataframe.jl

# runtime config
function conf(spark::SparkSession)
    jconf = jcall(spark.jspark, "conf", JRuntimeConfig, ())
    return RuntimeConfig(jconf)
end


function createDataFrame(spark::SparkSession, rows::Vector{Row}, sch::StructType)
    if !isempty(rows)
        row = rows[1]
        rsch = row.schema()
        if !isnothing(rsch) && rsch != sch
            @warn "Schema mismatch:\n\trow     : $(row.schema())\n\tprovided: $sch"
        end
    end
    jrows = [row.jrow for row in rows]
    jrows_arr = convert(JArrayList, jrows)
    jdf = jcall(spark.jspark, "createDataFrame", JDataset, (JList, JStructType), jrows_arr, sch.jst)
    return DataFrame(jdf)
end

function createDataFrame(spark::SparkSession, rows::Vector{Row}, sch::Union{String, Vector{String}})
    st = StructType(sch)
    return spark.createDataFrame(rows, st)
end

function createDataFrame(spark::SparkSession, data::Vector{Vector{Any}}, sch::Union{String, Vector{String}})
    rows = map(Row, data)
    st = StructType(sch)
    return spark.createDataFrame(rows, st)
end

function createDataFrame(spark::SparkSession, rows::Vector{Row})
    @assert !isempty(rows) "Cannot create a DataFrame from empty list of rows"
    st = rows[1].schema()
    return spark.createDataFrame(rows, st)
end

"Creates Spark DataFrame from the Julia table using Arrow.jl for data transfer."
function createDataFrame(spark::SparkSession, data::Tables.AbstractColumns)
    createDataFrameFromTable(spark, data)
end
"Creates Spark DataFrame from the Julia table using Arrow.jl for data transfer."
function createDataFrame(spark::SparkSession, data::DataFrames.AbstractDataFrame)
    createDataFrameFromTable(spark, data)
end

"Creates Spark DataFrame from any Tables.jl compatible table. Uses Arrow.jl for data transfer. When localRelation=true a LocalRelation is creates and Spark should be able to perform filter pushdown on JOINs with this DataFrame"
function createDataFrameFromTable(spark::SparkSession, table, localRelation=false)
    mktemp() do path,io
        Arrow.write(path, table; file=false)
        fn = if localRelation
            "fromArrow2"
        else
            "fromArrow1"
        end
        jdf = jcall(JDatasetUtils, fn, JDataset, (JSparkSession,JString), spark.jspark, path)
        DataFrame(jdf)
    end
end

function sql(spark::SparkSession, query::String)
    jdf = jcall(spark.jspark, "sql", JDataset, (JString,), query)
    return DataFrame(jdf)
end

###############################################################################
#                                RuntimeConfig                                #
###############################################################################

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

for JT in (JString, jlong, jboolean)
    T = java2julia(JT)
    @eval function set(cnf::RuntimeConfig, key::String, value::$T)
        jcall(cnf.jconf, "set", Nothing, (JString, $JT), key, value)
    end
end
