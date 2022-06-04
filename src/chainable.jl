"""
    DotChainer{O, Fn}

See `@chainable` for details.
"""
struct DotChainer{O, Fn}
    obj::O
    fn::Fn
end

# DotChainer(obj, fn) = DotChainer{typeof(obj), typeof(fn)}(obj, fn)

(c::DotChainer)(args...) = c.fn(c.obj, args...)


"""
    @chainable T

Adds dot chaining syntax to the type, i.e. automatically translate:

    foo.bar(a)

into

    bar(foo, a)

For single-argument functions also support implicit calls, e.g:

    foo.bar.baz(a, b)

is treated the same as:

    foo.bar().baz(a, b)

Note that `@chainable` works by overloading `Base.getproperty()`,
making it impossible to customize it for `T`. To have more control,
one may use the underlying wrapper type - `DotCaller`.
"""
macro chainable(T)
    return quote
        function Base.getproperty(obj::$(esc(T)), prop::Symbol)
            if hasfield(typeof(obj), prop)
                return getfield(obj, prop)
            elseif isdefined(@__MODULE__, prop)
                fn = getfield(@__MODULE__, prop)
                return DotChainer(obj, fn)
            else
                error("type $(typeof(obj)) has no field $prop")
            end
        end
    end
end


function Base.getproperty(dc::DotChainer, prop::Symbol)
    if hasfield(typeof(dc), prop)
        return getfield(dc, prop)
    else
        # implicitely call function without arguments
        # and propagate getproperty to the returned object
        return getproperty(dc(), prop)
    end
end