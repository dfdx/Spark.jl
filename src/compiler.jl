using JavaCall


const JFile = @jimport java.io.File
const JToolProvider = @jimport javax.tools.ToolProvider
const JJavaCompiler = @jimport javax.tools.JavaCompiler
const JInputStream = @jimport java.io.InputStream
const JOutputStream = @jimport java.io.OutputStream
const JClassLoader = @jimport java.lang.ClassLoader
const JURLClassLoader = @jimport java.net.URLClassLoader
const JURI = @jimport java.net.URI
const JURL = @jimport java.net.URL

const JUDF1 = @jimport org.apache.spark.sql.api.java.UDF1


java_typename(::Type{JavaObject{name}}) where name = string(name)

var_typename(tape::Tape, v::Variable) =
    JULIA_TO_JAVA_TYPES[typeof(tape[v].val)] |> java_typename
var_name(v::Variable) = string(Umlaut.make_name(v.id))


function JavaCall.classforname(name::String, loader)
    return jcall(JClass, "forName", JClass, (JString, jboolean, JClassLoader),
                 name, true, loader)
end


function mkclass(name::String, src::String)
    # based on https://stackoverflow.com/a/2946402
    cls = mktempdir(; prefix="jj-") do root
        # write source to a file
        elems = split(name, ".")
        pkg_path = joinpath(root, elems[1:end-1]...)
        mkpath(pkg_path)
        src_path = joinpath(pkg_path, elems[end] * ".java")
        open(src_path, "w") do f
            write(f, src)
        end
        # compile
        jcompiler = jcall(JToolProvider, "getSystemJavaCompiler", JJavaCompiler)
        jcall(jcompiler, "run", jint,
            (JInputStream, JOutputStream, JOutputStream, Vector{JString}),
            nothing, nothing, nothing, [src_path])
        # load class
        jfile = JFile((JString,), root)
        juri = jcall(jfile, "toURI", JURI)
        jurl = jcall(juri, "toURL", JURL)
        jloader = jcall(JURLClassLoader, "newInstance", JURLClassLoader, (Vector{JURL},), [jurl])
        classforname(name, jloader)
    end
    return cls
end


function instantiate(name::String, src::String)
    jclass = mkclass(name, src)
    return jcall(jclass, "newInstance", JObject, ())
end

function instantiate(src::String)
    pkg_name = match(r"package ([a-zA-z0-9_\.]+);", src).captures[1]
    class_name = match(r"class ([a-zA-z0-9_]+)", src).captures[1]
    return instantiate("$pkg_name.$class_name", src)
end


function jcall2(jobj::JavaObject, name::String, ret_type, arg_types, args...)
    jclass = getclass(jobj)
    jargs = [a for a in convert.(arg_types, args)]  # convert to Vector
    meth = jcall(jclass, "getMethod", JMethod, (JString, Vector{JClass}), name, getclass.(jargs))
    ret = meth(jobj, jargs...)
    return convert(ret_type, ret)
end


function to_java(tape::Tape)
    src = "package j2j;\n\n"
    src *= "import org.apache.spark.sql.api.java.UDF1;\n\n"
    # class name
    fn_name = string(tape[V(1)].val)
    class_name = fn_name * "_" * string(gensym())[3:end]
    # arguments
    arg_vars = inputs(tape)[2:end]
    arg_names = [var_name(v) for v in arg_vars]
    arg_types = [var_typename(tape, v) for v in arg_vars]
    arg_type_str = join(arg_types, ",")
    arg_and_types = join(["$T $n" for (n, T) in zip(arg_names, arg_types)], ",")
    # return value
    ret_name = var_name(tape.result)
    ret_type = var_typename(tape, tape.result)


    src *= "public class $class_name implements UDF1<$arg_type_str, $ret_type> {\n\n"
    src *= "    public $ret_type call($arg_and_types) {\n"

    src *= "    return 1.0;"  # TODO
    src *= "    }\n"
    src *= "}"
    return src
end


###############################################################################
#                                      UDF                                    #
###############################################################################

function udf(f, args...)
    val, tape = trace(f, args...)
end


function main_udf()
    f = (x) -> 2x + 1
    args = (2.0,)
end