
import Base.TcpSocket

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

function launch_worker()
    port = int(readline(STDIN))
    info("JuliaWorker: Connecting to port $(port)!")
    sock = connect("127.0.0.1", port)
    info("JuliaWorker: Connected!")
    part_id = readint(sock)
    info("JuliaWorker: partition id = $part_id")
    cmd = readobj(sock)
    info("JuliaWorker: cmd bytes = $cmd")
    writeobj(sock, "hello")
    writeobj(sock, "I'm working...")
    writeobj(sock, "good bye")
    info("JuliaWorker: Wrote object")
    write(sock, -1)
    info("JuliaWorker: Exiting")
end

