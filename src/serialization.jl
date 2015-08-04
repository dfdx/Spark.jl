
function serialized(f::Function)
    buf = IOBuffer()
    serialize(buf, f)
    return buf.data
end
