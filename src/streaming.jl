###############################################################################
#                                DataStreamReader                             #
###############################################################################

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


for (T, JT) in [(String, JString), (Integer, jlong), (Real, jdouble), (Bool, jboolean)]
    @eval function option(stream::DataStreamReader, key::String, value::$T)
        jcall(stream.jstream, "option", JDataStreamReader, (JString, $JT), key, value)
        return stream
    end
end


for func in (:csv, :json, :parquet, :orc, :text, :textFile)
    @eval function $func(stream::DataStreamReader, path::String)
        jdf = jcall(stream.jstream, string($func), JDataset, (JString,), path)
        return DataFrame(jdf)
    end
end


function load(stream::DataStreamReader, path::String)
    jdf = jcall(stream.jstream, "load", JDataset, (JString,), path)
    return DataFrame(jdf)
end

function load(stream::DataStreamReader)
    jdf = jcall(stream.jstream, "load", JDataset, ())
    return DataFrame(jdf)
end