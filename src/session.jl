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

sparkjl_jar() = joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.2.jar")

for JT in (JDouble, JLong, JBoolean)
    T = java2julia(JT)
    @eval function config(builder::SparkSessionBuilder, key::String, value::$T)
        jcall(builder.jbuilder, "config", JSparkSessionBuilder, (JString, $JT), key, value)
        return builder
    end
end


const SPARK_JARS = String[sparkjl_jar()]

function config(builder::SparkSessionBuilder, key::String, value::String)
    if key == "spark.jars"
        # User attempts to set spark.jars config. Later we also need to add
        # our own sparkjl.jar, but SparkSessionBuilder does allow to retrieve
        # previous value. Thus instead of setting the value immediately,
        # we save it to a global variable and use later in getOrCreate()
        push!(SPARK_JARS, split(value, ",")...)
    else
        jcall(builder.jbuilder, "config", JSparkSessionBuilder, (JString, JString), key, value)
    end
    return builder
end

function enableHiveSupport(builder::SparkSessionBuilder)
    jcall(builder.jbuilder, "enableHiveSupport", JSparkSessionBuilder, ())
    return builder
end

function getOrCreate(builder::SparkSessionBuilder)
    # set "spark.jars", including sparkjl.jar and any values
    # set via config(::SparkSessionBuilder, ::String, ::String)
    spark_jars = join(SPARK_JARS, ",")
    jcall(builder.jbuilder, "config", JSparkSessionBuilder, (JString, JString), "spark.jars", spark_jars)
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