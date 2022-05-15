import Spark.Compiler: jcall2, udf
import Spark.JavaCall: @jimport, jdouble, JString

const JDouble = @jimport java.lang.Double

@testset "Compiler" begin
    f = (x, y) -> 2x + y
    f_udf = udf(f, 2.0, 3.0)
    r = jcall2(f_udf.judf, "call", jdouble, (JDouble, JDouble), 5.0, 6.0)
    @test r == f(5.0, 6.0)

    f = s -> lowercase(s)
    f_udf = udf(f, "Hi!")
    r = jcall2(f_udf.judf, "call", JString, (JString,), "Big Buddha Boom!")
    @test convert(String, r) == f("Big Buddha Boom!")
end

