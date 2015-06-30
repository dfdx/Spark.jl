
function launch_worker()
    port = int(readline(STDIN))
    info("JuliaWorker: Connecting to port $(port)!")
    connect("127.0.0.1", port)
    info("JuliaWorker: Connected!")
end
