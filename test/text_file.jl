@testset "text_file" begin

# test of text_file
sc = SparkContext(master="local")

txt = text_file(sc, @__FILE__)
nums  = map(txt, it -> length(it))

@test reduce(nums, +) == 182

close(sc)

end
