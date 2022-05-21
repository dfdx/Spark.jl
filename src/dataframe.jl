###############################################################################
#                                  DataFrame                                  #
###############################################################################

Base.show(df::DataFrame) = jcall(df.jdf, "show", Nothing, ())
Base.show(io::IO, df::DataFrame) = show(df)
printSchema(df::DataFrame) = jcall(df.jdf, "printSchema", Nothing, ())


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

function take(df::DataFrame, n::Integer)
    return convert(Vector{Row}, jcall(df.jdf, "take", JObject, (jint,), n))
end


function describe(df::DataFrame, cols::String...)
    jdf = jcall(df.jdf, "describe", JDataset, (Vector{JString},), collect(cols))
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

###############################################################################
#                                  GroupedData                                #
###############################################################################

@chainable GroupedData
Base.show(io::IO, gdf::GroupedData) = print(io, "GroupedData()")

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

for func in (:min, :max,  :count, :sum, :mean)
    @eval function $func(gdf::GroupedData, cols::String...)
        jdf = jcall(gdf.jgdf, string($func), JDataset, (Vector{JString},), collect(cols))
        return DataFrame(jdf)
    end
end

minimum(gdf::GroupedData, cols::String...) = min(gdf, cols...)
maximum(gdf::GroupedData, cols::String...) = max(gdf, cols...)
avg(gdf::GroupedData, cols::String...) = mean(gdf, cols...)