
import Base.convert

# strings
# convert(::Type{String}, a::Array{Uint8}) = bytestring(a)

# ints
convert(::Type{Array{Uint8}}, n::Int) = error("not implemented")
convert(::Type{Int}, a::Type{Array{Uint8}}) = error("not implemented")
