package org.apache.spark.api.julia;

import org.apache.spark.sql.api.java.UDF1;

public class Gen1 implements UDF1<Integer, Integer> {
    @Override
    public Integer call(Integer obj) {
        return obj + 1;
    }
}