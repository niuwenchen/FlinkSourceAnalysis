package com.jackniu.flink.api.common.operators.util;

import java.io.Serializable;
import java.lang.annotation.Annotation;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface UserCodeWrapper<T> extends Serializable {
    /**
     * Gets the user code object, which may be either a function or an input or output format.
     * The subclass is supposed to just return the user code object or instantiate the class.
     *
     * @return The class with the user code.
     */
    T getUserCodeObject(Class<? super T> superClass, ClassLoader cl);

    /**
     * Gets the user code object. In the case of a pact, that object will be the stub with the user function,
     * in the case of an input or output format, it will be the format object.
     *
     * @return The class with the user code.
     */
    T getUserCodeObject();

    /**
     * Gets an annotation that pertains to the user code class. By default, this method will look for
     * annotations statically present on the user code class. However, inheritors may override this
     * behavior to provide annotations dynamically.
     *
     * @param annotationClass
     *        the Class object corresponding to the annotation type
     * @return the annotation, or null if no annotation of the requested type was found
     */
    <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass);

    /**
     * Gets the class of the user code. If the user code is provided as a class, this class is just returned.
     * If the user code is provided as an object, {@link Object#getClass()} is called on the user code object.
     *
     * @return The class of the user code object.
     */
    Class<? extends T> getUserCodeClass ();

    /**
     * Checks whether the wrapper already has an object, or whether it needs to instantiate it.
     *
     * @return True, if the wrapper has already an object, false if it has only a class.
     */
    boolean hasObject();
}
