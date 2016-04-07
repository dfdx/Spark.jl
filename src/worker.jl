
import Base.TCPSocket
using Iterators

const END_OF_DATA_SECTION = -1
const JULIA_EXCEPTION_THROWN = -2
const TIMING_DATA = -3
const END_OF_STREAM = -4
const NULL = -5

readint(sock::TCPSocket) = ntoh(read(sock, Int32))

"""
Read data object from a socket. Returns code and byte array:
 * if code is negative, it's considered as a special command code
 * if code is positive, it's considered as array length
"""
function readobj(sock::TCPSocket)
    len = readint(sock)
    bytes = len > 0 ? readbytes(sock, len) : []
    return len, bytes
end

writeint(sock::TCPSocket, x::Int) = write(sock, hton(Int32(x)))

"""Write length and byte array of that length to a socket"""
function writeobj(sock::TCPSocket, obj::Vector{UInt8})
    writeint(sock, length(obj))
    write(sock, obj)
end

writeobj(sock::TCPSocket, obj) = writeobj(sock, convert(Vector{UInt8}, obj))



function load_stream{T}(::Type{T}, sock::TCPSocket)
    function it()        
        code, _next = readobj(sock)
        while code != END_OF_DATA_SECTION
            data = from_bytes(T, _next)            
            produce(data)
            code, _next = readobj(sock)
        end
    end
    return Task(it)
end


function dump_stream(sock::TCPSocket, it)
    for v in it
        writeobj(sock, to_bytes(v))
    end
end


function launch_worker()
    port = parse(Int, readline(STDIN))    
    sock = connect("127.0.0.1", port)
    try
        split = readint(sock)
        info("Julia: starting partition id: $split")
        T = deserialize(sock)
        cmd = readobj(sock)[2]
        func = deserialize(IOBuffer(cmd))        
        it = load_stream(T, sock)
        dump_stream(sock, func(split, it))
        writeint(sock, END_OF_DATA_SECTION)
        writeint(sock, END_OF_STREAM)
        info("Julia: exiting")
    catch e
        # TODO: handle the case when JVM closes connection
        Base.show_backtrace(STDERR, catch_backtrace())
        writeint(sock, JULIA_EXCEPTION_THROWN)
        rethrow()
    end
end
