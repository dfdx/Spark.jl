
import Base.TcpSocket

const END_OF_DATA_SECTION = -1
const JULIA_EXCEPTION_THROWN = -2
const TIMING_DATA = -3
const END_OF_STREAM = -4
const NULL = -5

readint(sock::TcpSocket) = ntoh(read(sock, Int32))

function readobj(sock::TcpSocket)
    len = readint(sock)
    bytes = len > 0 ? readbytes(sock, len) : []
    return len, bytes
end

writeint(sock::TcpSocket, x::Int) = write(sock, hton(int32(x)))

function writeobj(sock::TcpSocket, obj::Vector{Uint8})
    writeint(sock, length(obj))
    write(sock, obj)
end

writeobj(sock::TcpSocket, obj) = writeobj(sock, convert(Vector{Uint8}, obj))


function load_stream(sock::TcpSocket)
    function it()
        code, _next = readobj(sock)
        while code != END_OF_DATA_SECTION
            produce(_next)
            code, _next = readobj(sock)
        end
    end
    return Task(it)
end


function dump_stream(sock::TcpSocket, it::Task)
    for v in it
        writeobj(sock, v)
    end
    info("JuliaWorker: iterator dumped")
end


function launch_worker()
    port = int(readline(STDIN))
    info("Julia: Connecting to port $(port)!")
    sock = connect("127.0.0.1", port)
    try
        part_id = readint(sock)
        info("Julia: partition id = $part_id")
        cmd = readobj(sock)
        # we need to get iterator to apply function to it,
        # but actually data is loaded actively (both ways)
        it = load_stream(sock)
        func = identity # TODO: decode command from JVM
        dump_stream(sock, func(it))        
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
