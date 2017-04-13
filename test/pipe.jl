# test pipe

const PIPETEST = joinpath(dirname(@__FILE__), "pipetest.sh")
const JULIA = joinpath(JULIA_HOME, "julia")

sc = SparkContext(master="local")

nums1 = parallelize(sc, 1:30)

@static if is_unix()
nums11 = collect(pipe(nums1, PIPETEST))
@test length(nums11) > 30
end

nums11 = collect(pipe(nums1, [JULIA, "-e", """while !eof(STDIN); print("J: ", readline(STDIN)); end"""]))
@test length(nums11) == 30
for l in nums11
    @test startswith(l, "J: ")
end

nums11 = collect(pipe(nums1, [JULIA, "-e", """while !eof(STDIN); print(ENV["HEADER"], readline(STDIN)); end"""], Dict{String,String}("HEADER"=>"J: ")))
@test length(nums11) == 30
for l in nums11
    @test startswith(l, "J: ")
end

@static if is_unix()
pnums1 = cartesian(nums1, nums1)
pnums11 = collect(pipe(pnums1, PIPETEST))
@test length(collect(pipe(pnums1, PIPETEST))) > 900
end

pnums11 = collect(pipe(pnums1, [JULIA, "-e", """while !eof(STDIN); print("J: ", readline(STDIN)); end"""]))
@test length(pnums11) == 900
for l in pnums11
    @test startswith(l, "J: ")
end

pnums11 = collect(pipe(pnums1, [JULIA, "-e", """while !eof(STDIN); print(ENV["HEADER"], readline(STDIN)); end"""], Dict{String,String}("HEADER"=>"J: ")))
@test length(pnums11) == 900
for l in pnums11
    @test startswith(l, "J: ")
end

close(sc)
