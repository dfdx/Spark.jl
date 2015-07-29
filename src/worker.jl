
import Base.TcpSocket

const END_OF_DATA_SECTION = -1
const JULIA_EXCEPTION_THROWN = -2
const TIMING_DATA = -3
const END_OF_STREAM = -4
const NULL = -5

readint(sock::TcpSocket) = ntoh(read(sock, Int32))

function readobj(sock::TcpSocket)
    len = readint(sock)
    bytes = readbytes(sock, len)
    return bytes
end

writeint(sock::TcpSocket, x::Int) = write(sock, hton(int32(x)))

function writeobj(sock::TcpSocket, obj::Vector{Uint8})
    writeint(sock, length(obj))
    write(sock, obj)
end

writeobj(sock::TcpSocket, obj) = writeobj(sock, convert(Vector{Uint8}, obj))


function load_stream(sock::TcpSocket)
    function _it()
        _next = readobj(sock)  # should it be here or in outer function?
        while _next != END_OF_DATA_SECTION
            produce(_next)
            _next = readobj(sock)
        end
    end
    return Task(_it)
end


function dump_stream(it, sock)
    # TODO
end


function launch_worker()
    port = int(readline(STDIN))
    info("JuliaWorker: Connecting to port $(port)!")
    sock = connect("127.0.0.1", port)
    try                
        part_id = readint(sock)
        info("JuliaWorker: partition id = $part_id")
        
        cmd = readobj(sock)
        info("JuliaWorker: command = $cmd")
        
        fst = readobj(sock)
        info("JuliaWorker: $fst")
        
        # we need to get iterator to apply function to it,
        # but actually data is loaded actively (both ways)        
        it = load_stream(sock)

        info("JuliaWorker: got iterator")

        for v in it
            info("JuliaWorker: $v")
        end

        writeobj(sock, "hello")
        writeobj(sock, "I'm working...")
        writeobj(sock, "good bye")
        info("JuliaWorker: Wrote object")
        writeint(sock, END_OF_DATA_SECTION)
        writeint(sock, END_OF_STREAM)
        info("JuliaWorker: Exiting")
    catch e
        # TODO: handle the case when JVM closes connection
        info("JuliaWorker: exception = $e")
        writeint(sock, JULIA_EXCEPTION_THROWN)
        # TODO: write stacktrace to the socket
        exit(-1)
    end
end
