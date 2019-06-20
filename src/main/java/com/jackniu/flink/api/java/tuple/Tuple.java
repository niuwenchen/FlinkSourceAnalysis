package com.jackniu.flink.api.java.tuple;

import com.jackniu.flink.types.NullFieldException;

/**
 * Created by JackNiu on 2019/6/20.
 */
public abstract class Tuple implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    public static final int MAX_ARITY = 25;

    public abstract <T> T getField(int pos);

    public <T> T getFieldNotNull(int pos){
        T field = getField(pos);
        if (field != null) {
            return field;
        } else {
            throw new NullFieldException(pos);
        }
    }
    public abstract <T> void setField(T value, int pos);
    public abstract int getArity();
    public abstract <T extends Tuple> T copy();
    @SuppressWarnings("unchecked")
    public static Class<? extends Tuple> getTupleClass(int arity) {
        if (arity < 0 || arity > MAX_ARITY) {
            throw new IllegalArgumentException("The tuple arity must be in [0, " + MAX_ARITY + "].");
        }
        return (Class<? extends Tuple>) CLASSES[arity];
    }
    public static Tuple newInstance(int arity) {
        switch (arity) {
            case 0: return Tuple0.INSTANCE;
//            case 1: return new Tuple1();
//            case 2: return new Tuple2();
//            case 3: return new Tuple3();
//            case 4: return new Tuple4();
//            case 5: return new Tuple5();
//            case 6: return new Tuple6();
//            case 7: return new Tuple7();
//            case 8: return new Tuple8();
//            case 9: return new Tuple9();
//            case 10: return new Tuple10();
//            case 11: return new Tuple11();
//            case 12: return new Tuple12();
//            case 13: return new Tuple13();
//            case 14: return new Tuple14();
//            case 15: return new Tuple15();
//            case 16: return new Tuple16();
//            case 17: return new Tuple17();
//            case 18: return new Tuple18();
//            case 19: return new Tuple19();
//            case 20: return new Tuple20();
//            case 21: return new Tuple21();
//            case 22: return new Tuple22();
//            case 23: return new Tuple23();
//            case 24: return new Tuple24();
//            case 25: return new Tuple25();
            default: throw new IllegalArgumentException("The tuple arity must be in [0, " + MAX_ARITY + "].");
        }
    }

    private static final Class<?>[] CLASSES = new Class<?>[] {
            Tuple0.class,
            Tuple2.class,
//            Tuple1.class, Tuple3.class, Tuple4.class, Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class, Tuple9.class, Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class, Tuple14.class, Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class, Tuple19.class, Tuple20.class, Tuple21.class, Tuple22.class, Tuple23.class, Tuple24.class, Tuple25.class
    };

}
