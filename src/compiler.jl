using JavaCall
import JavaCall: assertroottask_or_goodenv, assertloaded
using Umlaut

const JInMemoryJavaCompiler = @jimport org.mdkt.compiler.InMemoryJavaCompiler

# const JDynamicJavaCompiler = @jimport org.apache.spark.api.julia.DynamicJavaCompiler

# const JFile = @jimport java.io.File
# const JToolProvider = @jimport javax.tools.ToolProvider
# const JJavaCompiler = @jimport javax.tools.JavaCompiler
# const JInputStream = @jimport java.io.InputStream
# const JOutputStream = @jimport java.io.OutputStream
# const JClassLoader = @jimport java.lang.ClassLoader
# const JURLClassLoader = @jimport java.net.URLClassLoader
# const JURI = @jimport java.net.URI
# const JURL = @jimport java.net.URL

const JUDF1 = @jimport org.apache.spark.sql.api.java.UDF1


###############################################################################
#                                  Compiler                                   #
###############################################################################


function create_class(name::String, src::String)
    jcompiler = jcall(JInMemoryJavaCompiler, "newInstance", JInMemoryJavaCompiler, ())
    return jcall(jcompiler, "compile", JClass, (JString, JString), name, src)
end


function create_instance(name::String, src::String)
    jclass = create_class(name, src)
    return jcall(jclass, "newInstance", JObject, ())
end

function create_instance(src::String)
    pkg_name_match = match(r"package ([a-zA-z0-9_\.\$]+);", src)
    @assert !isnothing(pkg_name_match) "Cannot detect package name in the source:\n\n$src"
    pkg_name = pkg_name_match.captures[1]
    class_name_match = match(r"class ([a-zA-z0-9_\$]+)", src)
    @assert !isnothing(class_name_match) "Cannot detect class name in the source:\n\n$src"
    class_name = class_name_match.captures[1]
    return create_instance("$pkg_name.$class_name", src)
end


###############################################################################
#                                   jcall2                                    #
###############################################################################

function jcall_reflect(jobj::JavaObject, name::String, rettype, argtypes, args...)
    assertroottask_or_goodenv() && assertloaded()
    jclass = getclass(jobj)
    jargs = [a for a in convert.(argtypes, args)]  # convert to Vector
    meth = jcall(jclass, "getMethod", JMethod, (JString, Vector{JClass}), name, getclass.(jargs))
    ret = meth(jobj, jargs...)
    return convert(rettype, ret)
end

# jcall() fails to call methods of generated classes, jcall2() is a more robust version of it
# see https://github.com/JuliaInterop/JavaCall.jl/issues/166 for the details
function jcall2(jobj::JavaObject, name::String, rettype, argtypes, args...)
    try
        return jcall(jobj, name, rettype, argtypes, args...)
    catch
        return jcall_reflect(jobj, name, rettype, argtypes, args...)
    end
end


###############################################################################
#                                  JavaExpr                                   #
###############################################################################


javastring(::Type{JavaObject{name}}) where name = string(name)
javastring(::Nothing) = ""

javatype(tape::Tape, v::Variable) = julia2java(typeof(tape[v].val))
javaname(v::Variable) = string(Umlaut.make_name(v.id))
javaname(op::AbstractArray) = javaname(V(op))
javaname(x) = x   # literals


type_param_string(typeparams::Vector{String}) =
    isempty(typeparams) ? "" : "<$(join(typeparams, ", "))>"
type_param_string(typeparams::Vector) =
    isempty(typeparams) ? "" : "<$(join(map(javastring, typeparams), ", "))>"


abstract type JavaExpr end
Base.show(io::IO, ex::JavaExpr) = print(io, javastring(ex))

mutable struct JavaTypeExpr <: JavaExpr
    class::Type{<:JavaObject}
    typeparams::Vector    # String or Type{<:JavaObject}
end
JavaTypeExpr(JT::Type{<:JavaObject}) = JavaTypeExpr(JT, [])
Base.convert(::Type{JavaTypeExpr}, JT::Type{<:JavaObject}) = JavaTypeExpr(JT)
javastring(ex::JavaTypeExpr) = javastring(ex.class) * type_param_string(ex.typeparams)



mutable struct JavaCallExpr <: JavaExpr
    rettype::JavaTypeExpr
    ret::String
    this::Union{String, Any}  # name or constant
    method::String
    args::Vector              # names or constants
end
function javastring(ex::JavaCallExpr)
    R = javastring(ex.rettype)
    if !isnothing(match(r"^[\*\/+-]+$", ex.method))
        # binary operator
        return "$R $(ex.ret) = $(ex.this) $(ex.method) $(ex.args[1]);"
    else
        return "$R $(ex.ret) = $(ex.this).$(ex.method)($(join(ex.args, ", ")));"
    end
end

struct JavaReturnExpr <: JavaExpr
    ret::String
end
javastring(ex::JavaReturnExpr) = "return $(ex.ret);"


mutable struct JavaMethodExpr <: JavaExpr
    annotations::Vector{String}
    rettype::JavaTypeExpr
    name::String
    params::Vector{String}
    paramtypes::Vector{JavaTypeExpr}
    body::Vector
end
function javastring(ex::JavaMethodExpr)
    paramlist = join(["$(javastring(t)) $a" for (a, t) in zip(ex.params, ex.paramtypes)], ", ")
    result = isempty(ex.annotations) ? "" : "\t" * join(ex.annotations, "\n") * "\n"
    result *= "public $(javastring(ex.rettype)) $(ex.name)($paramlist) {\n"
    for subex in ex.body
        result *= "\t$(javastring(subex))\n"
    end
    result *= "}"
    return result
end


mutable struct JavaClassExpr <: JavaExpr
    name::String
    typeparams::Vector{String}
    extends::Union{JavaTypeExpr, Nothing}
    implements::Union{JavaTypeExpr, Nothing}
    methods::Vector{<:JavaMethodExpr}
end
function javastring(ex::JavaClassExpr)
    sep = findlast(".", ex.name)
    pkg_name, class_name = isnothing(sep) ? ("", ex.name) : (ex.name[1:sep.start-1], ex.name[sep.start+1:end])
    pkg_str = isempty(pkg_name) ? "" : "package $pkg_name;"
    extends_str = isnothing(ex.extends) ? "" : "extends $(javastring(ex.extends))"
    implements_str = isnothing(ex.implements) ? "" : "implements $(javastring(ex.implements))"
    methods_str = join(map(javastring, ex.methods), "\n\n")
    methods_str = replace(methods_str, "\n" => "\n\t")
    return """
    $pkg_str

    public class $class_name $extends_str $implements_str {
        $methods_str
    }
    """
end


###############################################################################
#                                Tape => JavaExpr                             #
###############################################################################


struct J2JContext end
function Umlaut.isprimitive(::J2JContext, f, args...)
    Umlaut.isprimitive(Umlaut.BaseCtx(), f, args...) && return true
    modl = parentmodule(typeof(f))
    modl in (Spark, Base.Unicode) && return true
    return false
end

javamethod(::typeof(+)) = "+"
javamethod(::typeof(*)) = "*"
javamethod(::typeof(lowercase)) = "toLowerCase"


function JavaCallExpr(tape::Tape, op::Call)
    ret = javaname(V(op))
    rettype = javatype(tape, V(op))
    this, args... = map(javaname, op.args)
    method = javamethod(op.fn)
    return JavaCallExpr(rettype, ret, this, method, args)
end

function JavaClassExpr(tape::Tape; method_name::String="(unspecified)")
    fn_name = string(tape[V(1)].val)
    cls = fn_name * "_" * string(gensym())[3:end]
    cls = replace(cls, "#" => "_")
    inp = inputs(tape)[2:end]
    params = [javaname(v) for v in inp]
    paramtypes = [javatype(tape, v) for v in inp]
    ret = javaname(tape.result)
    rettype = javatype(tape, tape.result)
    body = JavaExpr[JavaCallExpr(tape, op) for op in tape if !isa(op, Umlaut.Input)]
    push!(body, JavaReturnExpr(ret))
    meth_expr = JavaMethodExpr([], rettype, method_name, params, paramtypes, body)
    return JavaClassExpr(cls, [], nothing, nothing, [meth_expr])
end


###############################################################################
#                                      UDF                                    #
###############################################################################

struct UDF
    src::String
    judf::JavaObject
end
Base.show(io::IO, udf::UDF) = print(io, "UDF from:\n\n" * udf.src)


function udf(f::Function, args...)
    val, tape = trace(f, args...; ctx=J2JContext())
    class_expr = JavaClassExpr(tape)
    class_expr.name = "julia2java." * class_expr.name
    UT = JavaTypeExpr(
        JavaCall.jimport("org.apache.spark.sql.api.java.UDF$(length(args))"),
        [javastring(julia2java(typeof(x))) for x in [args...; val]]
    )
    class_expr.implements = UT
    meth_expr = class_expr.methods[1]
    meth_expr.name = "call"
    push!(meth_expr.annotations, "@Override")
    src = javastring(class_expr)
    judf = create_instance(src)
    return UDF(src, judf)
end
