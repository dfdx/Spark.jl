
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


## Java-compatible serialization. Use it for data

from_bytes(::Type{Vector{UInt8}}, arr::Vector{UInt8}) = arr
function from_bytes{T}(::Type{T}, arr::Vector{Any})
    map(T, arr)
end
from_bytes(::Type{String}, arr::Vector{UInt8}) = String(arr)
# from_bytes(::Type{ASCIIString}, arr::Vector{UInt8}) = bytestring(arr)
from_bytes{I<:Integer}(::Type{I}, arr::Vector{UInt8}) = begin
    io = IOBuffer(arr)
    ntoh(read(io, I))
end


to_bytes(x::Vector{UInt8}) = x
to_bytes(x::String) = convert(Vector{UInt8}, x)
# to_bytes(x::ASCIIString) = convert(Vector{UInt8}, x)
to_bytes{I<:Integer}(x::I) = begin
    io = IOBuffer()
    write(io, hton(x))
    io.data
end

# generic serialization & deserialization for Julia objects
from_bytes{T}(::Type{T}, arr::Vector{UInt8}) = deserialized(arr)
to_bytes(x) = serialized(x)
