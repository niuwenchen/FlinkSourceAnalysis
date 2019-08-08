package com.jackniu.flink.api.common.functions;

/**
 * Created by JackNiu on 2019/7/7.
 */

import com.jackniu.flink.annotations.Public;

/**
 * A broadcast variable initializer can be used to transform a broadcast variable
 * into another format during initialization. The transformed variable is shared
 * among the parallel instances of a function inside one TaskManager, the
 * same way as the plain broadcast variables (lists) are shared.
 *
 * <p>The broadcast variable initializer will in many cases only be executed by one
 * parallel function instance per TaskManager, when acquiring the broadcast variable
 * for the first time inside that particular TaskManager. It is possible that a
 * broadcast variable is read and initialized multiple times, if the tasks that use
 * the variables are not overlapping in their execution time; in such cases, it can
 * happen that one function instance released the broadcast variable, and another
 * function instance materializes it again.
 *
 * <p>This is an example of how to use a broadcast variable initializer, transforming
 * the shared list of values into a shared map.
 *
 * <pre>{@code
 * public class MyFunction extends RichMapFunction<Long, String> {
 *
 *     private Map<Long, String> map;
 *
 *     public void open(Configuration cfg) throws Exception {
 *         getRuntimeContext().getBroadcastVariableWithInitializer("mapvar",
 *             new BroadcastVariableInitializer<Tuple2<Long, String>, Map<Long, String>>() {
 *
 *                 public Map<Long, String> initializeBroadcastVariable(Iterable<Tuple2<Long, String>> data) {
 *                     Map<Long, String> map = new HashMap<>();
 *
 *                     for (Tuple2<Long, String> t : data) {
 *                         map.put(t.f0, t.f1);
 *                     }
 *
 *                     return map;
 *                 }
 *             });
 *     }
 *
 *     public String map(Long value) {
 *         // replace the long by the String, based on the map
 *         return map.get(value);
 *     }
 * }
 *
 * }</pre>
 *
 * @param <T> The type of the elements in the list of the original untransformed broadcast variable.
 * @param <O> The type of the transformed broadcast variable.
 */
@Public
@FunctionalInterface
public interface BroadcastVariableInitializer<T, O> {

    /**
     * The method that reads the data elements from the broadcast variable and
     * creates the transformed data structure.
     *
     * <p>The iterable with the data elements can be traversed only once, i.e.,
     * only the first call to {@code iterator()} will succeed.
     *
     * @param data The sequence of elements in the broadcast variable.
     * @return The transformed broadcast variable.
     */
    O initializeBroadcastVariable(Iterable<T> data);
}
