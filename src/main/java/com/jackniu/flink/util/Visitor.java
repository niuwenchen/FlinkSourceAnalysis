package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/7/8.
 */
public interface Visitor<T extends Visitable<T>> {

    /**
     * Method that is invoked on the visit before visiting and child nodes or descendant nodes.
     *
     * @return True, if the traversal should continue, false otherwise.
     */
    boolean preVisit(T visitable);

    /**
     * Method that is invoked after all child nodes or descendant nodes were visited.
     */
    void postVisit(T visitable);
}

