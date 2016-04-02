
import Base.TCPSocket
using Iterators

const END_OF_DATA_SECTION = -1
const JULIA_EXCEPTION_THROWN = -2
const TIMING_DATA = -3
const END_OF_STREAM = -4
const NULL = -5

readint(sock::TCPSocket) = ntoh(read(sock, Int32))

function readobj(sock::TCPSocket)
    len = readint(sock)
    bytes = len > 0 ? readbytes(sock, len) : []
    return len, bytes
end

writeint(sock::TCPSocket, x::Int) = write(sock, hton(Int32(x)))

function writeobj(sock::TCPSocket, obj::Vector{UInt8})
    writeint(sock, length(obj))
    write(sock, obj)
end

writeobj(sock::TCPSocket, obj) = writeobj(sock, convert(Vector{UInt8}, obj))


function load_stream(sock::TCPSocket)
    function it()
        code, _next = readobj(sock)
        while code != END_OF_DATA_SECTION
            produce(_next)
            code, _next = readobj(sock)
        end
    end
    return Task(it)
end


function dump_stream(sock::TCPSocket, it)
    for v in it
        writeobj(sock, v)
    end
end


function launch_worker()
    port = Int(readline(STDIN))
    info("Julia: Connecting to port $(port)!")
    sock = connect("127.0.0.1", port)
    try
        part_id = readint(sock)
        info("Julia: partition id = $part_id")
        cmd = readobj(sock)[2]
        print("Julia: command = $cmd")
        func = deserialize(IOBuffer(cmd))
        # we need to get iterator to apply function to it,
        # but actually data is loaded actively (both ways)
        it = load_stream(sock)
        dump_stream(sock, func(part_id, it))        
        writeint(sock, END_OF_DATA_SECTION)
        writeint(sock, END_OF_STREAM)
        info("Julia: Exiting")
    catch e
        # TODO: handle the case when JVM closes connection
        Base.show_backtrace(STDERR, catch_backtrace())        
        writeint(sock, JULIA_EXCEPTION_THROWN)
        rethrow()
    end
end


