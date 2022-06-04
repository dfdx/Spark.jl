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

function Row(vals::Vector)
    jseq = convert(JSeq, vals)
    jrow = jcall(JRow, "fromSeq", JRow, (JSeq,), jseq)
    return Row(jrow)
end

function Row(vals...)
    return Row(collect(vals))
end


function Base.show(io::IO, row::Row)
    str = jcall(row.jrow, "toString", JString, ())
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

function schema(row::Row)
    jst = jcall(row.jrow, "schema", JStructType, ())
    return isnull(jst) ? nothing : StructType(jst)
end


function Base.getproperty(row::Row, prop::Symbol)
    if hasfield(Row, prop)
        return getfield(row, prop)
    end
    sch = schema(row)
    if !isnothing(sch) && string(prop) in names(sch)
        return row[string(prop)]
    else
        fn = getfield(@__MODULE__, prop)
        return DotChainer(row, fn)
    end
end


Base.:(==)(row1::Row, row2::Row) =
    Bool(jcall(row1.jrow, "equals", jboolean, (JObject,), row2.jrow))