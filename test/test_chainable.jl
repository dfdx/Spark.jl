struct Foo
    x::Int
end
@chainable Foo

struct Bar
    a::Int
end
@chainable Bar

add(foo::Foo, y) = foo.x + y
to_bar(foo::Foo) = Bar(foo.x)
mul(bar::Bar, b) = bar.a * b


@testset "chainable" begin
    foo = Foo(2.0);
    y = rand(); b = rand()

    # field access
    @test foo.x == 2.0

    # dot syntax
    @test foo.add(y) == add(foo, y)

    # chained field access
    @test foo.to_bar().a == 2.0

    # chained dot syntax
    @test foo.to_bar().mul(b) == mul(foo.to_bar(), b)

    # implicit call
    @test foo.to_bar.mul(b) == mul(foo.to_bar(), b)

    # correct type
    @test foo.to_bar isa Spark.DotChainer

end