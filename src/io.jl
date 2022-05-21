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
