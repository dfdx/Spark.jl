###############################################################################
#                                DataStreamReader                             #
###############################################################################

Base.show(io::IO, stream::DataStreamReader) = print(io, "DataStreamReader()")
@chainable DataStreamReader


function readStream(spark::SparkSession)
    jreader = jcall(spark.jspark, "readStream", JDataStreamReader, ())
    return DataStreamReader(jreader)
end


function format(stream::DataStreamReader, fmt::String)
    jreader = jcall(stream.jreader, "format", JDataStreamReader, (JString,), fmt)
    return DataStreamReader(jreader)
end


function schema(stream::DataStreamReader, sch::StructType)
    jreader = jcall(stream.jreader, "schema", JDataStreamReader, (JStructType,), sch.jst)
    return DataStreamReader(jreader)
end

function schema(stream::DataStreamReader, sch::String)
    jreader = jcall(stream.jreader, "schema", JDataStreamReader, (JString,), sch)
    return DataStreamReader(jreader)
end


for (T, JT) in [(String, JString), (Integer, jlong), (Real, jdouble), (Bool, jboolean)]
    @eval function option(stream::DataStreamReader, key::String, value::$T)
        jreader = jcall(stream.jreader, "option", JDataStreamReader, (JString, $JT), key, value)
        return DataStreamReader(jreader)
    end
end


for func in (:csv, :json, :parquet, :orc, :text, :textFile)
    @eval function $func(stream::DataStreamReader, path::String)
        jdf = jcall(stream.jreader, string($func), JDataset, (JString,), path)
        return DataFrame(jdf)
    end
end


function load(stream::DataStreamReader, path::String)
    jdf = jcall(stream.jreader, "load", JDataset, (JString,), path)
    return DataFrame(jdf)
end

function load(stream::DataStreamReader)
    jdf = jcall(stream.jreader, "load", JDataset, ())
    return DataFrame(jdf)
end


###############################################################################
#                                DataStreamWriter                             #
###############################################################################

Base.show(io::IO, stream::DataStreamWriter) = print(io, "DataStreamWriter()")
@chainable DataStreamWriter


function format(writer::DataStreamWriter, fmt::String)
    jwriter = jcall(writer.jwriter, "format", JDataStreamWriter, (JString,), fmt)
    return DataStreamWriter(jwriter)
end


function outputMode(writer::DataStreamWriter, m::String)
    jwriter = jcall(writer.jwriter, "outputMode", JDataStreamWriter, (JString,), m)
    return DataStreamWriter(jwriter)
end


for (T, JT) in [(String, JString), (Integer, jlong), (Real, jdouble), (Bool, jboolean)]
    @eval function option(writer::DataStreamWriter, key::String, value::$T)
        jwriter = jcall(writer.jwriter, "option", JDataStreamWriter, (JString, $JT), key, value)
        return DataStreamWriter(jwriter)
    end
end


function foreach(writer::DataStreamWriter, jfew::JObject)
    # Spark doesn't automatically distribute dynamically created objects to workers
    # Thus I turn off this feature for now
    error("Not implemented yet")
    # JForeachWriter = @jimport(org.apache.spark.sql.ForeachWriter)
    # jfew = convert(JForeachWriter, jfew)
    # jwriter = jcall(writer.jwriter, "foreach", JDataStreamWriter, (JForeachWriter,), jfew)
    # return DataStreamWriter(jwriter)
end


function start(writer::DataStreamWriter)
    jquery = jcall(writer.jwriter, "start", JStreamingQuery, ())
    return StreamingQuery(jquery)
end



###############################################################################
#                                StreamingQuery                               #
###############################################################################

Base.show(io::IO, query::StreamingQuery) = print(io, "StreamingQuery()")
@chainable StreamingQuery


function awaitTermination(query::StreamingQuery)
    jcall(query.jquery, "awaitTermination", Nothing, ())
end


function awaitTermination(query::StreamingQuery, timeout::Integer)
    return Bool(jcall(query.jquery, "awaitTermination", jboolean, (jlong,), timeout))
end


isActive(query::StreamingQuery) = Bool(jcall(query.jquery, "isActive", jboolean, ()))
stop(query::StreamingQuery) = jcall(query.jquery, "stop", Nothing, ())

explain(query::StreamingQuery) = jcall(query.jquery, "explain", Nothing, ())
explain(query::StreamingQuery, extended::Bool) =
    jcall(query.jquery, "explain", Nothing, (jboolean,), extended)

# TODO: foreach, foreachBatch