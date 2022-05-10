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


function jcall2(jobj::JavaObject, name::String, ret_type, arg_types, args...)
    jclass = getclass(jobj)
    jargs = [a for a in convert.(arg_types, args)]  # convert to Vector
    meth = jcall(jclass, "getMethod", JMethod, (JString, Vector{JClass}), name, getclass.(jargs))
    ret = meth(jobj, jargs...)
    return convert(ret_type, ret)
end

