###############################################################################
#                                    Column                                   #
###############################################################################

function Column(name::String)
    jcol = jcall(JSQLFunctions, "col", JColumn, (JString,), name)
    return Column(jcol)
end

@chainable Column
function Base.show(io::IO, col::Column)
    name = jcall(col.jcol, "toString", JString, ())
    print(io, "col(\"$name\")")
end


# binary with JObject
for (func, name) in [(:+, "plus"), (:-, "minus"), (:*, "multiply"), (:/, "divide")]
    @eval function Base.$func(col::Column, obj::T) where T
        jres = jcall(col.jcol, $name, JColumn, (JObject,), obj)
        return Column(jres)
    end
end


alias(col::Column, name::String) =
    Column(jcall(col.jcol, "alias", JColumn, (JString,), name))

asc(col::Column) = Column(jcall(col.jcol, "asc", JColumn, ()))
asc_nulls_first(col::Column) = Column(jcall(col.jcol, "asc_nulls_first", JColumn, ()))
asc_nulls_last(col::Column) = Column(jcall(col.jcol, "asc_nulls_last", JColumn, ()))

between(col::Column, low, up) =
    Column(jcall(col.jcol, "between", JColumn, (JObject, JObject), low, up))

bitwiseAND(col::Column, other) =
    Column(jcall(col.jcol, "bitwiseAND", JColumn, (JObject,), other))
Base.:&(col::Column, other) = bitwiseAND(col, other)

bitwiseOR(col::Column, other) =
    Column(jcall(col.jcol, "bitwiseOR", JColumn, (JObject,), other))
Base.:|(col::Column, other) = bitwiseOR(col, other)

bitwiseXOR(col::Column, other) =
    Column(jcall(col.jcol, "bitwiseXOR", JColumn, (JObject,), other))
Base.:⊻(col::Column, other) = bitwiseXOR(col, other)


Base.contains(col::Column, other) =
    Column(jcall(col.jcol, "contains", JColumn, (JObject,), other))

desc(col::Column) = Column(jcall(col.jcol, "desc", JColumn, ()))
desc_nulls_first(col::Column) = Column(jcall(col.jcol, "desc_nulls_first", JColumn, ()))
desc_nulls_last(col::Column) = Column(jcall(col.jcol, "desc_nulls_last", JColumn, ()))

# dropFields should go here, but it's not in listmethods(col.jcol) ¯\_(ツ)_/¯

Base.endswith(col::Column, other) =
    Column(jcall2(col.jcol, "endsWith", JColumn, (JObject,), other))
Base.endswith(col::Column, other::Column) =
    Column(jcall(col.jcol, "endsWith", JColumn, (JColumn,), other.jcol))

eqNullSafe(col::Column, other) =
    Column(jcall(col.jcol, "eqNullSafe", JColumn, (JObject,), other))

Base.:(==)(col::Column, other) = Column(jcall(col.jcol, "equalTo", JColumn, (JObject,), other))
Base.:(!=)(col::Column, other) = Column(jcall(col.jcol, "notEqual", JColumn, (JObject,), other))

explain(col::Column, extended=false) = jcall(col.jcol, "explain", Nothing, (jboolean,), extended)

isNotNull(col::Column) = Column(jcall(col.jcol, "isNotNull", JColumn, ()))
isNull(col::Column) = Column(jcall(col.jcol, "isNull", JColumn, ()))

like(col::Column, s::String) = Column(jcall(col.jcol, "like", JColumn, (JString,), s))

otherwise(col::Column, other) =
    Column(jcall(col.jcol, "otherwise", JColumn, (JObject,), other))

over(col::Column) = Column(jcall(col.jcol, "over", JColumn, ()))

rlike(col::Column, s::String) = Column(jcall(col.jcol, "rlike", JColumn, (JString,), s))

Base.startswith(col::Column, other) =
    Column(jcall2(col.jcol, "startsWith", JColumn, (JObject,), other))
Base.startswith(col::Column, other::Column) =
    Column(jcall(col.jcol, "startsWith", JColumn, (JColumn,), other.jcol))

substr(col::Column, start::Column, len::Column) =
    Column(jcall(col.jcol, "substr", JColumn, (JColumn, JColumn), start.jcol, len.jcol))
substr(col::Column, start::Integer, len::Integer) =
    Column(jcall(col.jcol, "substr", JColumn, (jint, jint), start, len))

when(col::Column, condition::Column, value) =
    Column(jcall(col.jcol, "when", JColumn, (JColumn, JObject), condition.jcol, value))


## JSQLFunctions

upper(col::Column) =
    Column(jcall(JSQLFunctions, "upper", JColumn, (JColumn,), col.jcol))
Base.uppercase(col::Column) = upper(col)

lower(col::Column) =
    Column(jcall(JSQLFunctions, "lower", JColumn, (JColumn,), col.jcol))
Base.lowercase(col::Column) = lower(col)


for func in (:min, :max,  :count, :sum, :mean)
    @eval function $func(col::Column)
        jcol = jcall(JSQLFunctions, string($func), JColumn, (JColumn,), col.jcol)
        return Column(jcol)
    end
end

Base.minimum(col::Column) = min(col)
Base.maximum(col::Column) = max(col)
avg(col::Column) = mean(col)


explode(col::Column) =
    Column(jcall(JSQLFunctions, "explode", JColumn, (JColumn,), col.jcol))


function window(col::Column, w_dur::String, slide_dur::String, start_time::String)
    return Column(jcall(JSQLFunctions, "window", JColumn,
                        (JColumn, JString, JString, JString),
                        col.jcol, w_dur, slide_dur, start_time))
end

