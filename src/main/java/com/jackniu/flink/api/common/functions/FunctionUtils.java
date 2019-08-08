package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.configuration.Configuration;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class FunctionUtils {
    public static void openFunction(Function function, Configuration parameters) throws Exception{
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.open(parameters);
        }
    }

    public static void closeFunction(Function function) throws Exception{
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.close();
        }
    }

    public static void setFunctionRuntimeContext(Function function, RuntimeContext context){
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.setRuntimeContext(context);
        }
    }

    public static RuntimeContext getFunctionRuntimeContext(Function function, RuntimeContext defaultContext){
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            return richFunction.getRuntimeContext();
        }
        else {
            return defaultContext;
        }
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private FunctionUtils() {
        throw new RuntimeException();
    }

}
