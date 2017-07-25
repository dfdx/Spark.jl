
# Script to launch Julia worker process
# NOTE: this file should be run as a standalone script so that all included definitions
# are available in module Main

import Spark: readint, writeint, readobj, writeobj, load_stream, dump_stream
import Spark: END_OF_DATA_SECTION, END_OF_STREAM, JULIA_EXCEPTION_THROWN
using Iterators


println("@@@@@@@@@@@ before")
Spark.init()

println("@@@@@@@@@@ after")


# if there are any attached files in the worker directory, include them
for filename in readdir()
    if ismatch(r"attached_.{8}\.jl", filename)
        include(joinpath(pwd(), filename))
    end
end

function main()
    println("!!!!!!!!!!!!!! 0")
    port = parse(Int, readline(STDIN))
    println("!!!!!!!!!!!!!! 0.5")
    sock = connect("127.0.0.1", port)
    try
        println("!!!!!!!!!!!!!! 3")
        split = readint(sock)
        func = readobj(sock)[2]
        println("!!!!!!!!!!!!!! 3")
        itc = load_stream(sock)             # return chain representing partition iterator
        dump_stream(sock, func(split, itc))
        println("!!!!!!!!!!!!!! 3")
        writeint(sock, END_OF_DATA_SECTION)
        writeint(sock, END_OF_STREAM)
    catch e
        # TODO: handle the case when JVM closes connection
        io = IOBuffer()
        Base.show_backtrace(io, catch_backtrace())
        seekstart(io)
        bt = readstring(io)
        info(bt)
        writeint(sock, JULIA_EXCEPTION_THROWN)
        writeobj(sock, string(e) * bt)
    end
end


main()
