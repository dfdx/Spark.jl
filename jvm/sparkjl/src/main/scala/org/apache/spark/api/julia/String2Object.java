package org.apache.spark.api.julia;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

import java.util.function.Function;

import net.openhft.compiler.CompilerUtils;

class String2Object {

    public static Object create(String className, String code) {
        try {
            Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, code);
            Object obj = aClass.newInstance();
            return obj;
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public static UserDefinedFunction createUdf1(String code) {
        UDF1 obj = (UDF1) create("MyClass", code);
        return udf(obj, DataTypes.IntegerType);
    }


    public static void main(String[] args) {
        // dynamically you can call
        String className = "Gen1";
        // String code = "public class Gen1 implements Runnable {\n" +
        //                 "    public void run() {\n" +
        //                 "        System.out.println(\"Hello World\");\n" +
        //                 "    }\n" +
        //                 "}\n";
        String code = "import java.util.function.Function;\n" +
                        "public class Gen1 implements Function<Object, Object> {\n" +
                        "    @Override\n" +
                        "    public Object apply(Object obj) {\n" +
                        "        System.out.println(\"Hello World\");\n" +
                        "        return null;\n" +
                        "    }\n" +
                        "}\n";
        // Function fn = (Function) create(className, code);
        // fn.apply((Object) 1);
        // UserDefinedFunction fnUDF = udf(fn, DataTypes.IntegerType);
        // System.out.println(fnUDF);

        UserDefinedFunction fn = udf(new Gen1(), DataTypes.IntegerType);
        System.out.println(fn);
    }


}