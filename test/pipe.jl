# test pipe

const PIPETEST = joinpath(dirname(@__FILE__), "pipetest.sh")
const JULIA = joinpath(JULIA_HOME, "julia")

@static if is_windows()
const TESTJL = "\"while !eof(STDIN); print(\\\"Julia\\\", readline(STDIN)); end\""
const TESTJLENV = "\"while !eof(STDIN); print(ENV[\\\"HEADER\\\"], readline(STDIN)); end\""
else
const TESTJL = """while !eof(STDIN); print("Julia", readline(STDIN)); end"""
const TESTJLENV = """while !eof(STDIN); print(ENV["HEADER"], readline(STDIN)); end"""
end

sc = SparkContext(master="local")

nums1 = parallelize(sc, 1:30)

@static if is_unix()
nums11 = collect(pipe(nums1, PIPETEST))
@test length(nums11) > 30
end

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

@static if is_unix()
pnums11 = collect(pipe(pnums1, PIPETEST))
@test length(collect(pipe(pnums1, PIPETEST))) > 900
end

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
