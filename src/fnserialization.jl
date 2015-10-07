
foo_expr = (quote
    function foo(x)
        return x + 1
    end
end).args[2]


############################

function foo(x)
    return x + 1
end


function main()
    io = IOBuffer()
    fn_serialize(io, foo, (Int,))
    seek(io, 1)
    bar = fn_deserialize(io)
end


########### API ###########

function fn_serialize(io::IO, f::Function, dtypes::Tuple{Type})    
    serialize(io, f.env.name)
    serialize(io, dtypes)
    ast = code_lowered(f, dtypes)[1]
    serialize(io, ast)
end

function fn_deserialize(io::IO)
    name = bytestring(deserialize(io))
    dtypes = deserialize(io)
    ast = deserialize(io)
    # construct header AST
    fn_args = ast.args[1]
    header = Expr(:call, name, fn_args...)
    # construct body AST
    body = copy(ast.args[3])
    body.head = :block
    # construct whole function AST
    fn_expr = Expr(:function, header, body)
    return fn_expr
end


############################

function mymap(f, rdd)
    argtypes = (Int,)  # get type or RDD elements
    data = serfun(f, argtypes)
    # send to JuliaRDD
    
end

function usecase()
    rdd = collect(1:10)  # array has interface similar to RDD
    rdd2 = mymap(foo, rdd)
end


eval(Base.uncompressed_ast(f.code))
