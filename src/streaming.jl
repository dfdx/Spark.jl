const JDataStreamReader = @jimport org.apache.spark.sql.streaming.DataStreamReader


struct DataStreamReader
    jstream
end

Base.show(io::IO, stream::DataStreamReader) = print(io, "DataStreamReader()")
@chainable DataStreamReader


function readStream(spark::SparkSession)
    jstream = jcall(spark.jspark, "readStream", JDataStreamReader)
    return DataStreamReader(jstream)
end


function format(stream::DataStreamReader, fmt::String)
        jstream = jcall(stream.jstream, "format", JDataStreamReader, (JString,), fmt)
        return DataStreamReader(jstream)
end

function option(stream::DataStreamReader, key::String, val::String)
    jstream = jcall(stream.jstream, "option", JDataStreamReader, (JString, JString), key, val)
    return DataStreamReader(jstream)
end

function option(stream::DataStreamReader, key::String, val::Integer)
    jstream = jcall(stream.jstream, "option", JDataStreamReader, (JString, jint), key, val)
    return DataStreamReader(jstream)
end
