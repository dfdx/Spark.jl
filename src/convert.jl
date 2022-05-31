###############################################################################
#                                Conversions                                  #
###############################################################################

# Note: both - java.sql.Timestamp and Julia's DateTime don't have timezone.
# But when printing, java.sql.Timestamp will assume UTC and convert to your
# local time. To avoid confusion e.g. in REPL, try use fixed date in UTC
# or now(Dates.UTC)
Base.convert(::Type{JTimestamp}, x::DateTime) =
    JTimestamp((jlong,), floor(Int, datetime2unix(x)) * 1000)
Base.convert(::Type{DateTime}, x::JTimestamp) =
    unix2datetime(jcall(x, "getTime", jlong, ()) / 1000)

Base.convert(::Type{JDate}, x::Date) =
    JDate((jlong,), floor(Int, datetime2unix(DateTime(x))) * 1000)
Base.convert(::Type{Date}, x::JDate) =
    Date(unix2datetime(jcall(x, "getTime", jlong, ()) / 1000))


Base.convert(::Type{JObject}, x::Integer) = convert(JObject, convert(JLong, x))
Base.convert(::Type{JObject}, x::Real) = convert(JObject, convert(JDouble, x))
Base.convert(::Type{JObject}, x::DateTime) = convert(JObject, convert(JTimestamp, x))
Base.convert(::Type{JObject}, x::Date) = convert(JObject, convert(JDate, x))
Base.convert(::Type{JObject}, x::Column) = convert(JObject, x.jcol)

Base.convert(::Type{Row}, obj::JObject) = Row(convert(JRow, obj))

Base.convert(::Type{String}, obj::JString) = unsafe_string(obj)
Base.convert(::Type{Integer}, obj::JLong) = jcall(obj, "longValue", jlong, ())

julia2java(::Type{String}) = JString
julia2java(::Type{Int64}) = JLong
julia2java(::Type{Int32}) = JInt
julia2java(::Type{Float64}) = JDouble
julia2java(::Type{Float32}) = JFloat
julia2java(::Type{Bool}) = JBoolean
julia2java(::Type{Any}) = JObject

java2julia(::Type{JString}) = String
java2julia(::Type{JLong}) = Int64
java2julia(::Type{JInteger}) = Int32
java2julia(::Type{JDouble}) = Float64
java2julia(::Type{JFloat}) = Float32
java2julia(::Type{JBoolean}) = Bool
java2julia(::Type{JTimestamp}) = DateTime
java2julia(::Type{JDate}) = Date
java2julia(::Type{JObject}) = Any

julia2ddl(::Type{String}) = "string"
julia2ddl(::Type{Int64}) = "long"
julia2ddl(::Type{Int32}) = "int"
julia2ddl(::Type{Float64}) = "double"
julia2ddl(::Type{Float32}) = "float"
julia2ddl(::Type{Bool}) = "boolean"
julia2ddl(::Type{Dates.Date}) = "date"
julia2ddl(::Type{Dates.DateTime}) = "timestamp"


function JArray(x::Vector{T}) where T
    JT = T <: JavaObject ? T : julia2java(T)
    x = convert(Vector{JT}, x)
    sz = length(x)
    init_val = sz == 0 ? C_NULL : Ptr(x[1])
    arrayptr = JavaCall.JNI.NewObjectArray(sz, Ptr(JavaCall.metaclass(JT)), init_val)
    arrayptr === C_NULL && geterror()
    for i=2:sz
        JavaCall.JNI.SetObjectArrayElement(arrayptr, i-1, Ptr(x[i]))
    end
    return JavaObject{typeof(x)}(arrayptr)
end


function Base.convert(::Type{JSeq}, x::Vector)
    jarr = JArray(x)
    jobj = convert(JObject, jarr)
    jarrseq = jcall(JArraySeq, "make", JArraySeq, (JObject,), jobj)
    return jcall(jarrseq, "toSeq", JSeq, ())
    # jwa = jcall(JWrappedArray, "make", JWrappedArray, (JObject,), jobj)
    # jwa = jcall(JArraySeq, "make", JArraySeq, (JObject,), jobj)
    # return jcall(jwa, "toSeq", JSeq, ())
end

function Base.convert(::Type{JMap}, d::Dict)
    jmap = JHashMap(())
    for (k, v) in d
        jk, jv = convert(JObject, k), convert(JObject, v)
        jcall(jmap, "put", JObject, (JObject, JObject), jk, jv)
    end
    return jmap
end
