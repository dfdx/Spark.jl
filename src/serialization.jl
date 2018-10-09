using Serialization
## 1. Julia-only serialization. Use it for technical data (e.g. functions, codes, etc.)

"Return serialized object as an array of bytes"
function serialized(x)
    buf = IOBuffer()
    serialize(buf, x)
    return buf.data
end

"Return object deserialized from array of bytes"
function deserialized(x::Vector{UInt8})
    return deserialize(IOBuffer(x))
end
