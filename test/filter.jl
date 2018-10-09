@testset "filter" begin

# test filter
sc = SparkContext(master="local")

nums1 = parallelize(sc, 1:30)
n2 = filter(nums1, x->(x>10))
n3 = filter(n2, x->(x<15))

@test count(n2) == 20
@test count(n3) == 4

pnums1 = cartesian(nums1, nums1)
pn2 = filter(pnums1, x->(x[1]>10 && x[2]<10))
pn3 = filter(pnums1, x->(x[1]<15 && x[2]>5))
@test count(pn2) == 180
@test count(pn3) == 350

close(sc)

end
