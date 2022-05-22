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
    @eval function option(reader::DataStreamReader, key::String, value::$T)
        jcall(reader.jstream, "option", JDataStreamReader, (JString, $JT), key, value)
        return reader
    end
end

# function option(stream::DataStreamReader, key::String, val::String)
#     jstream = jcall(stream.jstream, "option", JDataStreamReader, (JString, JString), key, val)
#     return DataStreamReader(jstream)
# end

# function option(stream::DataStreamReader, key::String, val::Integer)
#     jstream = jcall(stream.jstream, "option", JDataStreamReader, (JString, jint), key, val)
#     return DataStreamReader(jstream)
# end


### #




for func in (:csv, :json, :parquet, :orc, :text, :textFile)
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