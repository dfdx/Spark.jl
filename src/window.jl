###############################################################################
#                              Window & WindowSpec                            #
###############################################################################

@chainable WindowSpec
# @chainable Type{Window}

function Base.getproperty(W::Type{Window}, prop::Symbol)
    if hasfield(typeof(W), prop)
        return getfield(W, prop)
    elseif prop in (:currentRow, :unboundedFollowing, :unboundedPreceding)
        return jcall(JWindow, string(prop), jlong, ())
    else
        fn = getfield(@__MODULE__, prop)
        return DotChainer(W, fn)
    end
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


Base.show(io::IO, win::Window) = print(io, "Window()")
Base.show(io::IO, win::WindowSpec) = print(io, "WindowSpec()")

for (WT, jobj) in [(WindowSpec, :(win.jwin)), (Type{Window}, JWindow)]
    @eval function orderBy(win::$WT, cols::Column...)
        jwin = jcall($jobj, "orderBy", JWindowSpec,
                    (Vector{JColumn},), [col.jcol for col in cols])
        return WindowSpec(jwin)
    end

    @eval function orderBy(win::$WT, col::String, cols::String...)
        jwin = jcall($jobj, "orderBy", JWindowSpec, (JString, Vector{JString},), col, collect(cols))
        return WindowSpec(jwin)
    end

    @eval function partitionBy(win::$WT, cols::Column...)
        jwin = jcall($jobj, "partitionBy", JWindowSpec,
                    (Vector{JColumn},), [col.jcol for col in cols])
        return WindowSpec(jwin)
    end

    @eval function partitionBy(win::$WT, col::String, cols::String...)
        jwin = jcall($jobj, "partitionBy", JWindowSpec, (JString, Vector{JString},), col, collect(cols))
        return WindowSpec(jwin)
    end

    @eval function rangeBetween(win::$WT, start::Column, finish::Column)
        jwin = jcall($jobj, "rangeBetween", JWindowSpec, (JColumn, JColumn), start.jcol, finish.jcol)
        return WindowSpec(jwin)
    end

    @eval function rangeBetween(win::$WT, start::Integer, finish::Integer)
        jwin = jcall($jobj, "rangeBetween", JWindowSpec, (jlong, jlong), start, finish)
        return WindowSpec(jwin)
    end

    @eval function rowsBetween(win::$WT, start::Column, finish::Column)
        jwin = jcall($jobj, "rowsBetween", JWindowSpec, (JColumn, JColumn), start.jcol, finish.jcol)
        return WindowSpec(jwin)
    end

    @eval function rowsBetween(win::$WT, start::Integer, finish::Integer)
        jwin = jcall($jobj, "rowsBetween", JWindowSpec, (jlong, jlong), start, finish)
        return WindowSpec(jwin)
    end

end

