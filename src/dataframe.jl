###############################################################################
#                                  DataFrame                                  #
###############################################################################


import Arrow
import DataFrames
import Tables

Base.show(df::DataFrame) = jcall(df.jdf, "show", Nothing, ())
Base.show(df::DataFrame, n::Integer) = jcall(df.jdf, "show", Nothing, (jint,), n)
function Base.show(io::IO, df::DataFrame)
    if df.isstreaming()
        print(io, toString(df.jdf))
    else
        show(df)
    end
end
printSchema(df::DataFrame) = jcall(df.jdf, "printSchema", Nothing, ())


function Base.getindex(df::DataFrame, name::String)
    jcol = jcall(df.jdf, "col", JColumn, (JString,), name)
    return Column(jcol)
end

function Base.propertynames(df::DataFrame, private::Bool=false)
    columns = Symbol.(columns(df))
    properties = [ :jdf, :printSchema, :show, :count, :first, :head, :collect, :collect_tuples, :collect_df, :collect_arrow, :take, :describe, :alias, :select, :withColumn, :filter, :where, :groupBy, :min, :max, :count, :sum, :mean, :minimum, :maximum, :avg, :createOrReplaceTempView, :isStreaming, :writeStream, :write ]
    return vcat(columns, properties)
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
    jnames = jcall(df.jdf, "columns", Vector{JString}, ())
    names = [unsafe_string(jn) for jn in jnames]
    return names
end


Base.count(df::DataFrame) = jcall(df.jdf, "count", jlong, ())
Base.first(df::DataFrame) = Row(jcall(df.jdf, "first", JObject, ()))

head(df::DataFrame) = Row(jcall(df.jdf, "head", JObject, ()))
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

function Base.collect(df::DataFrame, col::Union{<:AbstractString, <:Integer})
    rows = collect(df)
    return [row[col] for row in rows]
end

"""
Returns an array of named tuples that contains all rows in this DataFrame.
```
julia> spark.sql("select 1 as a, 'x' as b, array(1, 2, 3) as c").collect_tuples()
1-element Vector{NamedTuple{(:a, :b, :c), Tuple{Int32, String, Vector{Int32}}}}:
 (a = 1, b = "x", c = [1, 2, 3])
```
"""
function collect_tuples(ds::DataFrame)
    Tables.rowtable(collect_arrow(ds))
end

"""
Returns a DataFrame from DataFrames.jl that contains all rows in this Spark DataFrame.
```
julia> spark.sql("select 1 as a, 'x' as b, array(1, 2, 3) as c").collect_df()

1×3 DataFrame
 Row │ a      b       c              
     │ Int32  String  Array…         
─────┼───────────────────────────────
   1 │     1  x       Int32[1, 2, 3]

```
"""
function collect_df(ds::DataFrame)
    DataFrames.DataFrame(collect_arrow(ds))
end


"""Returns an Arrow.Table that contains all rows in this Dataset.
   This function will be slightly faster than collect_to_dataframe."""
function collect_arrow(ds::DataFrame)
    mktemp() do path,io
        jcall(JDatasetUtils, "collectToArrow2", Nothing, (JDataset, JString, jboolean), ds.jdf, path, false)
        Arrow.Table(path)
    end
end

function take(df::DataFrame, n::Integer)
    return convert(Vector{Row}, jcall(df.jdf, "take", JObject, (jint,), n))
end


function describe(df::DataFrame, cols::String...)
    jdf = jcall(df.jdf, "describe", JDataset, (Vector{JString},), collect(cols))
    return DataFrame(jdf)
end


function alias(df::DataFrame, name::String)
    jdf = jcall(df.jdf, "alias", JDataset, (JString,), name)
    return DataFrame(jdf)
end


function select(df::DataFrame, cols::Column...)
    jdf = jcall(df.jdf, "select", JDataset, (Vector{JColumn},),
                [col.jcol for col in cols])
    return DataFrame(jdf)
end
select(df::DataFrame, cols::String...) = select(df, map(Column, cols)...)


function withColumn(df::DataFrame, name::String, col::Column)
    jdf = jcall(df.jdf, "withColumn", JDataset, (JString, JColumn), name, col.jcol)
    return DataFrame(jdf)
end


function Base.filter(df::DataFrame, col::Column)
    jdf = jcall(df.jdf, "filter", JDataset, (JColumn,), col.jcol)
    return DataFrame(jdf)
end
where(df::DataFrame, col::Column) = filter(df, col)

function groupby(df::DataFrame, cols::Column...)
    jgdf = jcall(df.jdf, "groupBy", JRelationalGroupedDataset,
            (Vector{JColumn},), [col.jcol for col in cols])
    return GroupedData(jgdf)
end

function groupby(df::DataFrame, col::String, cols::String...)
    jgdf = jcall(df.jdf, "groupBy", JRelationalGroupedDataset,
            (JString, Vector{JString},), col, collect(cols))
    return GroupedData(jgdf)
end

const groupBy = groupby

for func in (:min, :max,  :count, :sum, :mean)
    @eval function $func(df::DataFrame, cols::String...)
        jdf = jcall(df.jdf, string($func), JDataset, (Vector{JString},), collect(cols))
        return DataFrame(jdf)
    end
end

minimum(df::DataFrame, cols::String...) = min(df, cols...)
maximum(df::DataFrame, cols::String...) = max(df, cols...)
avg(df::DataFrame, cols::String...) = mean(df, cols...)


function Base.join(df1::DataFrame, df2::DataFrame, col::Column, typ::String="inner")
    jdf = jcall(df1.jdf, "join", JDataset,
            (JDataset, JColumn, JString),
            df2.jdf, col.jcol, typ)
    return DataFrame(jdf)
end

createOrReplaceTempView(df::DataFrame, name::AbstractString) =
    jcall(df.jdf, "createOrReplaceTempView", Nothing, (JString,), name)


isstreaming(df::DataFrame) = Bool(jcall(df.jdf, "isStreaming", jboolean, ()))
isStreaming(df::DataFrame) = isstreaming(df)


function writeStream(df::DataFrame)
    jwriter = jcall(df.jdf, "writeStream", JDataStreamWriter, ())
    return DataStreamWriter(jwriter)
end


###############################################################################
#                                  GroupedData                                #
###############################################################################

@chainable GroupedData
function Base.propertynames(gdf::GroupedData, private::Bool=false)
    [:show, :agg, :min, :max, :sum, :mean, :minimum, :maximum, :avg, :count]
end

function Base.show(io::IO, gdf::GroupedData)
    repr = jcall(gdf.jgdf, "toString", JString, ())
    repr = replace(repr, "RelationalGroupedDataset" => "GroupedData")
    print(io, repr)
end

function agg(gdf::GroupedData, col::Column, cols::Column...)
    jdf = jcall(gdf.jgdf, "agg", JDataset,
            (JColumn, Vector{JColumn}), col.jcol, [col.jcol for col in cols])
    return DataFrame(jdf)
end

function agg(gdf::GroupedData, ops::Dict{<:AbstractString, <:AbstractString})
    jmap = convert(JMap, ops)
    jdf = jcall(gdf.jgdf, "agg", JDataset, (JMap,), jmap)
    return DataFrame(jdf)
end

for func in (:min, :max, :sum, :mean)
    @eval function $func(gdf::GroupedData, cols::String...)
        jdf = jcall(gdf.jgdf, string($func), JDataset, (Vector{JString},), collect(cols))
        return DataFrame(jdf)
    end
end

minimum(gdf::GroupedData, cols::String...) = min(gdf, cols...)
maximum(gdf::GroupedData, cols::String...) = max(gdf, cols...)
avg(gdf::GroupedData, cols::String...) = mean(gdf, cols...)

Base.count(gdf::GroupedData) =
    DataFrame(jcall(gdf.jgdf, "count", JDataset, ()))


function write(df::DataFrame)
    jwriter = jcall(df.jdf, "write", JDataFrameWriter, ())
    return DataFrameWriter(jwriter)
end
