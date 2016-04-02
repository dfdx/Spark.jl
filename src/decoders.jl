
decode(::Type{Vector{UInt8}}, arr::Vector{UInt8}) = arr
decode(::Type{UTF8String}, arr::Vector{UInt8}) = utf8(arr)
