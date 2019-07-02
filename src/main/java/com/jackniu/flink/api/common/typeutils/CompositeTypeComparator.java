package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.annotations.PublicEvolving;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by JackNiu on 2019/7/1.
 */
@SuppressWarnings("rawtypes")
@PublicEvolving
public abstract class CompositeTypeComparator<T> extends TypeComparator<T>  {
    private static final long serialVersionUID = 1L;

    @Override
    public TypeComparator[] getFlatComparators() {
        List<TypeComparator> flatComparators = new LinkedList<TypeComparator>();
        this.getFlatComparator(flatComparators);
        return flatComparators.toArray(new TypeComparator[flatComparators.size()]);
    }

    public abstract void getFlatComparator(List<TypeComparator> flatComparators);
}
