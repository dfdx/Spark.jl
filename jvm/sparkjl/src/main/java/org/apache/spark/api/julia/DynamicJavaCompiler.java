package org.apache.spark.api.julia;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

public class DynamicJavaCompiler {


    // private static Map<String, Object> cache = new HashMap<>();




    // public static Class<?> createClass(String source) throws Exception {
    //     // extract package and class names
    //     "package ([a-zA-z0-9_\.\$]+);"


    //     File root = Files.createTempDirectory("sparkjl-").toFile();
    //     // Save source in .java file.
    //     // File root = new File("/java"); // On Windows running on C:\, this is C:\java.
    //     File sourceFile = new File(root, "test/Test.java");
    //     sourceFile.getParentFile().mkdirs();
    //     Files.write(sourceFile.toPath(), source.getBytes(StandardCharsets.UTF_8));

    //     // Compile source file.
    //     JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    //     compiler.run(null, null, null, sourceFile.getPath());

    //     // Load and instantiate compiled class.
    //     URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { root.toURI().toURL() });
    //     Class<?> cls = Class.forName("test.Test", true, classLoader); // Should print "hello".
    //     return cls;
    //     // Object instance = cls.getDeclaredConstructor().newInstance(); // Should print "world".
    //     // System.out.println(instance); // Should print "test.Test@hashcode".
    // }

    // public static Object createInstance(String source) throws Exception {
    //     Class<?> cls = createClass(source);
    //     return cls.getDeclaredConstructor().newInstance();
    // }

    // public static Object getOrCreate(String source) throws Exception {
    //     if (!cache.containsKey(source)) {
    //         Object obj = createInstance(source);
    //         cache.put(source, obj);
    //     }
    //     return cache.get(source);
    // }
}