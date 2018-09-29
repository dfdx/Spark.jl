

"Iterates over the iterators within an iterator"
struct FlatMapIterator{I}
    it::I
end

Base.eltype(::Type{FlatMapIterator{I}}) where {I} = eltype(eltype(I))
Base.IteratorSize(::Type{FlatMapIterator{I}}) where {I} = Base.SizeUnknown()
Base.IteratorEltype(::Type{FlatMapIterator{I}}) where {I} = _flatteneltype(I, Base.IteratorEltype(I))
_flatteneltype(I, ::Base.HasEltype) = Base.IteratorEltype(eltype(I))
_flatteneltype(I, et) = Base.EltypeUnknown()

function Base.iterate(f::FlatMapIterator, state=((), nothing, nothing))
    state_outer, inner, next_inner = state
    if next_inner != nothing
        value_inner, state_inner = next_inner
        return value_inner, (state_outer, inner, iterate(inner, state_inner))
    end

    next_outer = iterate(f.it, state_outer...)
    while next_outer != nothing
        inner, state_outer = next_outer
        next_inner = iterate(inner)
        if next_inner != nothing
            value_inner, state_inner = next_inner
            return value_inner, (state_outer, inner, iterate(inner, state_inner))
        end
        next_outer = iterate(f.it, state_outer)
    end
    return nothing
end
