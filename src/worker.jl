
# import Base.IO
using Iterators

const END_OF_DATA_SECTION = -1
const JULIA_EXCEPTION_THROWN = -2
const TIMING_DATA = -3
const END_OF_STREAM = -4
const NULL = -5

readint(io::IO) = ntoh(read(io, Int32))

"""
Read data object from a ioet. Returns code and byte array:
 * if code is negative, it's considered as a special command code
 * if code is positive, it's considered as array length
"""
function readobj(io::IO)
    len = readint(io)
    bytes = len > 0 ? read(io, len) : []
    return len, bytes
end

writeint(io::IO, x::Int) = write(io, hton(Int32(x)))

"""Write length and byte array of that length to a ioet"""
function writeobj(io::IO, obj::Vector{UInt8})
    writeint(io, length(obj))
    write(io, obj)
end

writeobj(io::IO, obj) = writeobj(io, convert(Vector{UInt8}, obj))



function load_stream{T}(::Type{T}, io::IO)
    function it()        
        code, _next = readobj(io)
        while code != END_OF_DATA_SECTION
            data = from_bytes(T, _next)            
            produce(data)
            code, _next = readobj(io)
        end
    end
    return Task(it)
end


function dump_stream(io::IO, it)
    for v in it
        writeobj(io, to_bytes(v))
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
        T = deserialize(sock)
        cmd = readobj(sock)[2]
        func = deserialize(IOBuffer(cmd))        
        it = load_stream(T, sock)
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
