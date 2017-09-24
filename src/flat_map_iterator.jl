

"Iterates over the iterators within an iterator"
immutable FlatMapIterator{I}
    it::I
end

Base.eltype{I}(::Type{FlatMapIterator{I}}) = eltype(eltype(I))
Base.iteratorsize{I}(::Type{FlatMapIterator{I}}) = Base.SizeUnknown()
Base.iteratoreltype{I}(::Type{FlatMapIterator{I}}) = _flatteneltype(I, Base.iteratoreltype(I))
_flatteneltype(I, ::Base.HasEltype) = Base.iteratoreltype(eltype(I))
_flatteneltype(I, et) = Base.EltypeUnknown()

function Base.start(f::FlatMapIterator)
    local inner, s2
    s = start(f.it)
    d = done(f.it, s)
    d && return nothing
    done_inner = false
    while !d
        inner, s = next(f.it, s)
        s2 = start(inner)
        done_inner = done(inner, s2)
        !done_inner && break
        d = done(f.it, s)
    end
    return (d & done_inner), s, inner, s2
end

@inline function Base.next(f::FlatMapIterator, state::NTuple{4,Any})
    _done, s, inner, s2 = state
    val, s2 = next(inner, s2)
    done_inner = done_outer = false
    while (done_inner=done(inner, s2)) && !(done_outer=done(f.it, s))
        inner, s = next(f.it, s)
        s2 = start(inner)
    end
    return val, ((done_inner & done_outer), s, inner, s2)
end

Base.done(f::FlatMapIterator, state::Void) = true

@inline function Base.done(f::FlatMapIterator, state)
    return state[1]
end
