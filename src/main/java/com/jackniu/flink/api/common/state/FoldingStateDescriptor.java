package com.jackniu.flink.api.common.state;

import com.jackniu.flink.api.common.functions.FoldFunction;
import com.jackniu.flink.api.common.functions.RichFunction;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;

import static java.util.Objects.requireNonNull;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class FoldingStateDescriptor<T, ACC> extends StateDescriptor<FoldingState<T, ACC>, ACC> {
    private static final long serialVersionUID = 1L;


    private final FoldFunction<T, ACC> foldFunction;

    /**
     * Creates a new {@code FoldingStateDescriptor} with the given name, type, and initial value.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #FoldingStateDescriptor(String, ACC, FoldFunction, TypeInformation)} constructor.
     *
     * @param name The (unique) name for the state.
     * @param initialValue The initial value of the fold.
     * @param foldFunction The {@code FoldFunction} used to aggregate the state.
     * @param typeClass The type of the values in the state.
     */
    public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, Class<ACC> typeClass) {
        super(name, typeClass, initialValue);
        this.foldFunction = requireNonNull(foldFunction);

        if (foldFunction instanceof RichFunction) {
            throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
        }
    }

    /**
     * Creates a new {@code FoldingStateDescriptor} with the given name and default value.
     *
     * @param name The (unique) name for the state.
     * @param initialValue The initial value of the fold.
     * @param foldFunction The {@code FoldFunction} used to aggregate the state.
     * @param typeInfo The type of the values in the state.
     */
    public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, TypeInformation<ACC> typeInfo) {
        super(name, typeInfo, initialValue);
        this.foldFunction = requireNonNull(foldFunction);

        if (foldFunction instanceof RichFunction) {
            throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
        }
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name and default value.
     *
     * @param name The (unique) name for the state.
     * @param initialValue The initial value of the fold.
     * @param foldFunction The {@code FoldFunction} used to aggregate the state.
     * @param typeSerializer The type serializer of the values in the state.
     */
    public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, TypeSerializer<ACC> typeSerializer) {
        super(name, typeSerializer, initialValue);
        this.foldFunction = requireNonNull(foldFunction);

        if (foldFunction instanceof RichFunction) {
            throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
        }
    }

    /**
     * Returns the fold function to be used for the folding state.
     */
    public FoldFunction<T, ACC> getFoldFunction() {
        return foldFunction;
    }

    @Override
    public Type getType() {
        return Type.FOLDING;
    }
}
