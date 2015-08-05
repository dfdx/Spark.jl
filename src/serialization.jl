
@doc "Return serialized function as an array of bytes" ->
function serialized(f::Function)
    buf = IOBuffer()
    serialize(buf, f)
    return buf.data
end
