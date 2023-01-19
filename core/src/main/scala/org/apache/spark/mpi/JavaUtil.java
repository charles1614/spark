package org.apache.spark.mpi;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

public class JavaUtil {
    @SuppressWarnings("unchecked")
    public static Map<String, String> getModifiableEnvironment() throws Exception
    {
        Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
        Method getenv = pe.getDeclaredMethod("getenv", String.class);
        getenv.setAccessible(true);
        Field props = pe.getDeclaredField("theCaseInsensitiveEnvironment");
        props.setAccessible(true);
        return (Map<String, String>) props.get(null);
    }
}
