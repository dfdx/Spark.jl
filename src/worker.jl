
function launch_worker()
    port = int(readline(STDIN))
    info("JuliaWorker: Connecting to port $(port)!")
    sock = connect("127.0.0.1", port)
    info("JuliaWorker: Connected!")
    write(sock, -1)
end
