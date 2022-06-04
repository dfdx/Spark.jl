###############################################################################
#                                  StructType                                 #
###############################################################################

StructType() = StructType(JStructType(()))

function StructType(flds::StructField...)
    st = StructType()
    for fld in flds
        st = add(st, fld)
    end
    return st
end

function StructType(sch::Vector{<:AbstractString})
    flds = StructField[]
    for name_ddl in sch
        name, ddl = split(strip(name_ddl), " ")
        push!(flds, StructField(name, ddl, true))
    end
    return StructType(flds...)
end

function StructType(sch::String)
    return StructType(split(sch, ","))
end


@chainable StructType
Base.show(io::IO, st::StructType) = print(io, jcall(st.jst, "toString", JString, ()))

fieldNames(st::StructType) = convert(Vector{String}, jcall(st.jst, "fieldNames", Vector{JString}, ()))
Base.names(st::StructType) = fieldNames(st)


add(st::StructType, sf::StructField) =
    StructType(jcall(st.jst, "add", JStructType, (JStructField,), sf.jsf))

Base.getindex(st::StructType, idx::Integer) =
    StructField(jcall(st.jst, "apply", JStructField, (jint,), idx - 1))

Base.getindex(st::StructType, name::String) =
    StructField(jcall(st.jst, "apply", JStructField, (JString,), name))


Base.:(==)(st1::StructType, st2::StructType) =
    Bool(jcall(st1.jst, "equals", jboolean, (JObject,), st2.jst))

###############################################################################
#                                  StructField                                #
###############################################################################

function StructField(name::AbstractString, typ::AbstractString, nullable::Bool)
    dtyp = jcall(JDataType, "fromDDL", JDataType, (JString,), typ)
    empty_metadata = jcall(JMetadata, "empty", JMetadata, ())
    jsf = jcall(
        JStructField, "apply", JStructField,
        (JString, JDataType, jboolean, JMetadata),
        name, dtyp, nullable, empty_metadata
    )
    return StructField(jsf)
end

Base.show(io::IO, sf::StructField) = print(io, jcall(sf.jsf, "toString", JString, ()))

Base.:(==)(st1::StructField, st2::StructField) =
    Bool(jcall(st1.jsf, "equals", jboolean, (JObject,), st2.jsf))