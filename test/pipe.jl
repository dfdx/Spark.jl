# test pipe

const JULIA = joinpath(JULIA_HOME, "julia")

@static if is_windows()
    const PIPETEST = joinpath(dirname(@__FILE__), "pipetest.cmd")
    const TESTJL = "\"while !eof(STDIN); print(\\\"Julia\\\", readline(STDIN; chomp=false)); end\""
    const TESTJLENV = "\"while !eof(STDIN); print(ENV[\\\"HEADER\\\"], readline(STDIN; chomp=false)); end\""
else
    const PIPETEST = joinpath(dirname(@__FILE__), "pipetest.sh")
    const TESTJL = """while !eof(STDIN); print("Julia", readline(STDIN; chomp=false)); end"""
    const TESTJLENV = """while !eof(STDIN); print(ENV["HEADER"], readline(STDIN; chomp=false)); end"""
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
