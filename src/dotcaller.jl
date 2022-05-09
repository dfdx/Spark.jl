struct DotCaller{O, Fn}
    obj::O
    fn::Fn
end

DotCaller(obj, fn) = DotCaller{typeof(obj), typeof(fn)}(obj, fn)

(c::DotCaller)(args...) = c.fn(c.obj, args...)



macro dot_call(T)
    return quote
        function Base.getproperty(obj::$T, prop::Symbol)
            if prop in names(@__MODULE__)
                fn = getfield(@__MODULE__, prop)
                return DotCaller(obj, fn)
            else
                return getfield(obj, prop)
            end
        end
    end
end
