# # const JFile = @jimport java.io.File
# const JToolProvider = @jimport javax.tools.ToolProvider
# const JJavaCompiler = @jimport javax.tools.JavaCompiler
# const JInputStream = @jimport java.io.InputStream
# const JOutputStream = @jimport java.io.OutputStream
# const JArray = @jimport java.lang.reflect.Array

const JInMemoryJavaCompiler = @jimport org.mdkt.compiler.InMemoryJavaCompiler

# const JUDF1 = @jimport org.apache.spark.sql.api.java.UDF1


function mkclass(name::String, src::String)
    jcompiler = jcall(JInMemoryJavaCompiler, "newInstance", JInMemoryJavaCompiler, ())
    return jcall(jcompiler, "compile", JClass, (JString, JString), name, src)
end


function instantiate(name::String, src::String)
    jclass = mkclass(name, src)
    return jcall(jclass, "newInstance", JObject, ())
end


function main()
    init()
    name = "julia.compiled.Dummy"
    src = """
        package julia.compiled;

        import java.util.function.Function;

        public class Dummy implements Function<String, String> {

            @Override
            public String apply(String name) {
                return "Hello, " + name;
            }

            public void hello() {
                System.out.println("Hello!");
            }
        }
    """
    jc = mkclass(name, src)
    jo = jcall(jc, "newInstance", JObject, ())
    # jo = instantiate(name, src)
    # can't call inherited methods like this?
    jcall(jo, "apply", JString, (JString,), "Lee")
    jcall2(jo, "hello", Nothing, ())
    jcall(jc, "getMethods", Vector{JMethod}, ())
end


function jcall2(jobj::JavaObject, name::String, ret_type, arg_types, args...)
    jclass = getclass(jobj)
    jargs = [a for a in convert.(arg_types, args)]  # convert to Vector
    meth = jcall(jclass, "getMethod", JMethod, (JString, Vector{JClass}), name, getclass.(jargs))
    return meth(jobj, jargs...)
end