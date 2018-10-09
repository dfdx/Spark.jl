@testset "pipe" begin

# test pipe

JULIA = joinpath(get(ENV, "JULIA_HOME", ""), "julia")

READLINE_CMD = "readline(stdin; keep=true)"

@static if Sys.iswindows()
    PIPETEST = joinpath(dirname(@__FILE__), "pipetest.cmd")
    TESTJL = "\"while !eof(stdin); print(\\\"Julia\\\", $READLINE_CMD); end\""
    TESTJLENV = "\"while !eof(stdin); print(ENV[\\\"HEADER\\\"], $READLINE_CMD); end\""
else
    PIPETEST = joinpath(dirname(@__FILE__), "pipetest.sh")
    TESTJL = """while !eof(stdin); print("Julia", $READLINE_CMD); end"""
    TESTJLENV = """while !eof(stdin); print(ENV["HEADER"], $READLINE_CMD); end"""
end

sc = SparkContext(master="local")

nums1 = parallelize(sc, 1:30)

nums11 = collect(pipe(nums1, PIPETEST))
@test length(nums11) > 30

nums11 = collect(pipe(nums1, [JULIA, "-e", TESTJL]))
@test length(nums11) == 30
for l in nums11
    @test startswith(l, "Julia")
end

nums11 = collect(pipe(nums1, [JULIA, "-e", TESTJLENV], Dict{String,String}("HEADER"=>"Julia")))
@test length(nums11) == 30
for l in nums11
    @test startswith(l, "Julia")
end

pnums1 = cartesian(nums1, nums1)

pnums11 = collect(pipe(pnums1, PIPETEST))
@test length(pnums11) > 900

pnums11 = collect(pipe(pnums1, [JULIA, "-e", TESTJL]))
@test length(pnums11) == 900
for l in pnums11
    @test startswith(l, "Julia")
end

pnums11 = collect(pipe(pnums1, [JULIA, "-e", TESTJLENV], Dict{String,String}("HEADER"=>"Julia")))
@test length(pnums11) == 900
for l in pnums11
    @test startswith(l, "Julia")
end

close(sc)

end
