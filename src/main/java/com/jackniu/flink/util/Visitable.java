package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/7/8.
 */
public interface Visitable<T extends Visitable<T>> {

    /**
     * Contains the logic to invoke the visitor and continue the traversal.
     * Typically invokes the pre-visit method of the visitor, then sends the visitor to the children (or predecessors)
     * and then invokes the post-visit method.
     *
     * <p>A typical code example is the following:
     * <pre>{@code
     * public void accept(Visitor<Operator> visitor) {
     *     boolean descend = visitor.preVisit(this);
     *     if (descend) {
     *         if (this.input != null) {
     *             this.input.accept(visitor);
     *         }
     *         visitor.postVisit(this);
     *     }
     * }
     * }</pre>
     *
     * @param visitor The visitor to be called with this object as the parameter.
     *
     * @see Visitor#preVisit(Visitable)
     * @see Visitor#postVisit(Visitable)
     */
    void accept(Visitor<T> visitor);
}

