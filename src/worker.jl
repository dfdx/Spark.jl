
# import Base.IO
using Iterators

const END_OF_DATA_SECTION = -1
const JULIA_EXCEPTION_THROWN = -2
const TIMING_DATA = -3
const END_OF_STREAM = -4
const NULL = -5
const PAIR_TUPLE = -6
const ARRAY_VALUE = -7
const ARRAY_END = -8
const STRING_START = -100

readint(io::IO) = ntoh(read(io, Int32))

"""
Read data object from a ioet. Returns code and byte array:
 * if code is negative, it's considered as a special command code
 * if code is positive, it's considered as array length
"""
function readobj(io::IO)
    len = readint(io)
    if(len > 0)
        (len , deserialized(read(io, len)))
    elseif(len == PAIR_TUPLE)
        res = (readobj(io)[2], readobj(io)[2])
        (len , res)
    elseif(len == ARRAY_VALUE)
        arr = Any[]
        while len == ARRAY_VALUE
            append!(arr, readobj(io)[2])
            len = readint(io)
        end
        (len, arr)
    elseif(len == ARRAY_END)
        (len, Any[])
    elseif(len < STRING_START)
        (len , String(read(io, -len + STRING_START)))
    elseif(len == STRING_START)
        (len, "")
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

function load_stream(io::IO)
    function it()        
        code, _next = readobj(io)
        while code != END_OF_DATA_SECTION
            produce(_next)
            code, _next = readobj(io)
        end
    end
    return Task(it)
end


function dump_stream(io::IO, it)
    for v in it
        writeobj(io, v)
    end
end


function include_attached()
    for filename in readdir()
        if ismatch(r"attached_.{8}\.jl", filename)
            include(filename)
        end
    end
end


function launch_worker()
    include_attached()
    port = parse(Int, readline(STDIN))    
    sock = connect("127.0.0.1", port)
    try
        split = readint(sock)
        # info("Julia: starting partition id: $split")
        func = readobj(sock)[2]
        it = load_stream(sock)
        dump_stream(sock, func(split, it))
        writeint(sock, END_OF_DATA_SECTION)
        writeint(sock, END_OF_STREAM)
        # info("Julia: exiting")
    catch e
        # TODO: handle the case when JVM closes connection
        io = IOBuffer()
        Base.show_backtrace(io, catch_backtrace())
        seekstart(io)
        bt = readall(io)
        info(bt)
        write(STDERR, bt)
        writeint(sock, JULIA_EXCEPTION_THROWN)
        rethrow()
    end
end
