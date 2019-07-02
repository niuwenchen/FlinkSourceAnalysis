package com.jackniu.flink.types;

import com.jackniu.flink.annotations.Internal;
import com.jackniu.flink.annotations.Public;
import com.jackniu.flink.api.common.typeinfo.TypeInfo;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.java.typeutils.EitherTypeInfoFactory;

/**
 * Created by JackNiu on 2019/7/1.
 */
@Public
@TypeInfo(EitherTypeInfoFactory.class)
public abstract class Either<L,R> {
    /**
     * Create a Left value of Either
     */
    public static <L, R> Either<L, R> Left(L value) {
        return new Left<L, R>(value);
    }

    /**
     * Create a Right value of Either
     */
    public static <L, R> Either<L, R> Right(R value) {
        return new Right<L, R>(value);
    }

    /**
     * Retrieve the Left value of Either.
     *
     * @return the Left value
     * @throws IllegalStateException
     *             if called on a Right
     */
    public abstract L left() throws IllegalStateException;

    /**
     * Retrieve the Right value of Either.
     *
     * @return the Right value
     * @throws IllegalStateException
     *             if called on a Left
     */
    public abstract R right() throws IllegalStateException;

    /**
     *
     * @return true if this is a Left value, false if this is a Right value
     */
    public final boolean isLeft() {
        return getClass() == Left.class;
    }

    /**
     *
     * @return true if this is a Right value, false if this is a Left value
     */
    public final boolean isRight() {
        return getClass() == Right.class;
    }

    /**
     * A left value of {@link Either}
     *
     * @param <L>
     *            the type of Left
     * @param <R>
     *            the type of Right
     */
    public static class Left<L, R> extends Either<L, R> {
        private L value;

        private Right<L, R> right;

        public Left(L value) {
            this.value = java.util.Objects.requireNonNull(value);
        }

        @Override
        public L left() {
            return value;
        }

        @Override
        public R right() {
            throw new IllegalStateException("Cannot retrieve Right value on a Left");
        }

        /**
         * Sets the encapsulated value to another value
         *
         * @param value the new value of the encapsulated value
         */
        public void setValue(L value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof Left<?, ?>) {
                final Left<?, ?> other = (Left<?, ?>) object;
                return value.equals(other.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return "Left(" + value.toString() + ")";
        }

        /**
         * Creates a left value of {@link Either}
         *
         */
        public static <L, R> Left<L, R> of(L left) {
            return new Left<L, R>(left);
        }
    }

    /**
     * A right value of {@link Either}
     *
     * @param <L>
     *            the type of Left
     * @param <R>
     *            the type of Right
     */
    public static class Right<L, R> extends Either<L, R> {
        private R value;

        private Left<L, R> left;

        public Right(R value) {
            this.value = java.util.Objects.requireNonNull(value);
        }

        @Override
        public L left() {
            throw new IllegalStateException("Cannot retrieve Left value on a Right");
        }

        @Override
        public R right() {
            return value;
        }

        /**
         * Sets the encapsulated value to another value
         *
         * @param value the new value of the encapsulated value
         */
        public void setValue(R value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof Right<?, ?>) {
                final Right<?, ?> other = (Right<?, ?>) object;
                return value.equals(other.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return "Right(" + value.toString() + ")";
        }

        /**
         * Creates a right value of {@link Either}
         *
         */
        public static <L, R> Right<L, R> of(R right) {
            return new Right<L, R>(right);
        }
    }

    /**
     * Utility function for {@link } to support object reuse.
     *
     * To support object reuse both subclasses of Either contain a reference to
     * an instance of the other type. This method provides access to and
     * initializes the cross-reference.
     *
     * @param input container for Left or Right value
     * @param leftSerializer for creating an instance of the left type
     * @param <L>
     *            the type of Left
     * @param <R>
     *            the type of Right
     * @return input if Left type else input's Left reference
     */
    @Internal
    public static <L, R> Left<L, R> obtainLeft(Either<L, R> input, TypeSerializer<L> leftSerializer) {
        if (input.isLeft()) {
            return (Left<L, R>) input;
        } else {
            Right<L, R> right = (Right<L, R>) input;
            if (right.left == null) {
                right.left = Left.of(leftSerializer.createInstance());
                right.left.right = right;
            }
            return right.left;
        }
    }

    /**
     * Utility function for {@link } to support object reuse.
     *
     * To support object reuse both subclasses of Either contain a reference to
     * an instance of the other type. This method provides access to and
     * initializes the cross-reference.
     *
     * @param input container for Left or Right value
     * @param rightSerializer for creating an instance of the right type
     * @param <L>
     *            the type of Left
     * @param <R>
     *            the type of Right
     * @return input if Right type else input's Right reference
     */
    @Internal
    public static <L, R> Right<L, R> obtainRight(Either<L, R> input, TypeSerializer<R> rightSerializer) {
        if (input.isRight()) {
            return (Right<L, R>) input;
        } else {
            Left<L, R> left = (Left<L, R>) input;
            if (left.right == null) {
                left.right = Right.of(rightSerializer.createInstance());
                left.right.left = left;
            }
            return left.right;
        }
    }

}
