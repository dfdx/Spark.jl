"Iterates over the iterators within an iterator"
type FlatMapIterator
    outer_iterator::Any
end

"""
Returns a state tuple that represents the next available actual value.
Each state tuple contains 
  - the iterator state for the outer iterator
  - the inner iterator
  - the state of the inner iterator
"""
function get_state_for_next_value(fmi::FlatMapIterator, outer_state)
    if !done(fmi.outer_iterator, outer_state)
        next_outer = next(fmi.outer_iterator, outer_state)
        next_inner_state = start(next_outer[1])
        if !done(next_outer[1], next_inner_state)
            # found something
            (next_outer[2], next_outer[1], next_inner_state)
        else
            # inner is empty so move to next outer
            get_state_for_next_value(fmi::FlatMapIterator, next_outer[2])
        end
    else
        # we've reached the end so flag using Void
        (outer_state, Void, Void)
    end
end

function Base.start(fmi::FlatMapIterator)
    get_state_for_next_value(fmi, start(fmi.outer_iterator))
end

function Base.next(fmi::FlatMapIterator, state)
    next_inner = next(state[2], state[3])
    if !done(state[2], next_inner[2])
        # move onto next value in inner iterator
        (next_inner[1], (state[1], state[2], next_inner[2]))
    else
        # move onto next value using outer iterator
        (next_inner[1], get_state_for_next_value(fmi, state[1]))
    end
end

function Base.done(fmi::FlatMapIterator, state)
    state[2] == Void
end
