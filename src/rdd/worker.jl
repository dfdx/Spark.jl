
# import Base.IO

const END_OF_DATA_SECTION = -1
const JULIA_EXCEPTION_THROWN = -2
const TIMING_DATA = -3
const END_OF_STREAM = -4
const NULL = -5
const PAIR_TUPLE = -6
const ARRAY_VALUE = -7
const ARRAY_END = -8
const INTEGER = -9
const STRING_START = -100

readint(io::IO) = ntoh(read(io, Int32))

"""
Read data object from a ioet. Returns code and byte array:
 * if code is negative, it's considered as a special command code
 * if code is positive, it's considered as array length
"""
function readobj(io::IO)
    len = readint(io)
    if (len > 0)
        (len , deserialized(read(io, len)))
    elseif (len == PAIR_TUPLE)
        res = (readobj(io)[2], readobj(io)[2])
        (len , res)
    elseif (len == ARRAY_VALUE)
        arr = Any[]
        while len == ARRAY_VALUE
            push!(arr, readobj(io)[2])
            len = readint(io)
        end
        (len, arr)
    elseif (len == ARRAY_END)
        (len, Any[])
    elseif (len < STRING_START)
        (len , String(read(io, -len + STRING_START)))
    elseif (len == STRING_START)
        (len, "")
    elseif (len == INTEGER)
        (len, ntoh(read(io, Int64)))
    else
        (len, [])
    end
end

writeint(io::IO, x::Int) = write(io, hton(Int32(x)))

"""Write object to stream"""
function writeobj(io::IO, obj::Any)
    sobj = serialized(obj)
    writeint(io, length(sobj))
    write(io, sobj)
end

function writeobj(io::IO, obj::Tuple{Any,Any})
    writeint(io, PAIR_TUPLE)
    writeobj(io, obj[1])
    writeobj(io, obj[2])
end

function writeobj(io::IO, str::AbstractString)
    utf8 = Vector(transcode(UInt8, str))
    writeint(io, STRING_START - length(utf8))
    write(io, utf8)
end

function writeobj(io::IO, x::Integer)
    writeint(io, INTEGER)
    write(io, hton(Int64(x)))
end


function load_stream(io::IO)
    ch = Channel{Any}(1024)
    code, _next = readobj(io)
    @async begin
        while code != END_OF_DATA_SECTION
            put!(ch, _next)
            code, _next = readobj(io)
        end
        close(ch)
    end
    return ch
end


function dump_stream(io::IO, it)
    for v in it
        writeobj(io, v)
    end
end

