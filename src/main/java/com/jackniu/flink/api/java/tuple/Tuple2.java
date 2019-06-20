package com.jackniu.flink.api.java.tuple;


import com.jackniu.flink.util.StringUtils;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class Tuple2<T0,T1> extends Tuple {
    private static final long serialVersionUID = 1L;

    /** Field 0 of the tuple. */
    public T0 f0;
    /** Field 1 of the tuple. */
    public T1 f1;
    /**
     * Creates a new tuple where all fields are null.
     */
    public Tuple2() {}

    /**
     * Creates a new tuple and assigns the given values to the tuple's fields.
     *
     * @param value0 The value for field 0
     * @param value1 The value for field 1
     */
    public Tuple2(T0 value0, T1 value1) {
        this.f0 = value0;
        this.f1 = value1;
    }

    @Override
    public int getArity() {
        return 2;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(int pos) {
        switch(pos) {
            case 0: return (T) this.f0;
            case 1: return (T) this.f1;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void setField(T value, int pos) {
        switch(pos) {
            case 0:
                this.f0 = (T0) value;
                break;
            case 1:
                this.f1 = (T1) value;
                break;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    public void setFields(T0 value0, T1 value1) {
        this.f0 = value0;
        this.f1 = value1;
    }
    public Tuple2<T1, T0> swap() {
        return new Tuple2<T1, T0>(f1, f0);
    }
    @Override
    public String toString() {
        return "(" + StringUtils.arrayAwareToString(this.f0)
                + "," + StringUtils.arrayAwareToString(this.f1)
                + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Tuple2)) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        Tuple2 tuple = (Tuple2) o;
        if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) {
            return false;
        }
        if (f1 != null ? !f1.equals(tuple.f1) : tuple.f1 != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = f0 != null ? f0.hashCode() : 0;
        result = 31 * result + (f1 != null ? f1.hashCode() : 0);
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple2<T0, T1> copy() {
        return new Tuple2<>(this.f0,
                this.f1);
    }

    /**
     * Creates a new tuple and assigns the given values to the tuple's fields.
     * This is more convenient than using the constructor, because the compiler can
     * infer the generic type arguments implicitly. For example:
     * {@code Tuple3.of(n, x, s)}
     * instead of
     * {@code new Tuple3<Integer, Double, String>(n, x, s)}
     */
    public static <T0, T1> Tuple2<T0, T1> of(T0 value0, T1 value1) {
        return new Tuple2<>(value0,
                value1);
    }


}
