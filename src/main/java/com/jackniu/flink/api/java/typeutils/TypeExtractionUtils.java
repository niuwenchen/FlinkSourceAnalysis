package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.api.common.functions.Function;
import com.jackniu.flink.api.common.functions.InvalidTypesException;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.jackniu.flink.shaded.asm5.org.objectweb.asm.Type.getConstructorDescriptor;
import static com.jackniu.flink.shaded.asm5.org.objectweb.asm.Type.getMethodDescriptor;

/**
 * Created by JackNiu on 2019/6/24.
 */
public class TypeExtractionUtils {
    private TypeExtractionUtils() {
        // do not allow instantiation
    }

    /**
     * Similar to a Java 8 Executable but with a return type.
     */
    public static class LambdaExecutable {

        private Type[] parameterTypes;
        private Type returnType;
        private String name;
        private Object executable;

        public LambdaExecutable(Constructor<?> constructor) {
            this.parameterTypes = constructor.getGenericParameterTypes();
            this.returnType = constructor.getDeclaringClass();
            this.name = constructor.getName();
            this.executable = constructor;
        }

        public LambdaExecutable(Method method) {
            this.parameterTypes = method.getGenericParameterTypes();
            this.returnType = method.getGenericReturnType();
            this.name = method.getName();
            this.executable = method;
        }

        public Type[] getParameterTypes() {
            return parameterTypes;
        }

        public Type getReturnType() {
            return returnType;
        }

        public String getName() {
            return name;
        }

        public boolean executablesEquals(Method m) {
            return executable.equals(m);
        }

        public boolean executablesEquals(Constructor<?> c) {
            return executable.equals(c);
        }
    }

    public static LambdaExecutable checkAndExtractLambda(Function function) throws TypeExtractionException {
        try {
            // get serialized lambda
            SerializedLambda serializedLambda = null;
            for (Class<?> clazz = function.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
                try {
                    Method replaceMethod = clazz.getDeclaredMethod("writeReplace");
                    replaceMethod.setAccessible(true);
                    Object serialVersion = replaceMethod.invoke(function);

                    // check if class is a lambda function
                    if (serialVersion != null && serialVersion.getClass() == SerializedLambda.class) {
                        serializedLambda = (SerializedLambda) serialVersion;
                        break;
                    }
                }
                catch (NoSuchMethodException e) {
                    // thrown if the method is not there. fall through the loop
                }
            }

            // not a lambda method -> return null
            if (serializedLambda == null) {
                return null;
            }

            // find lambda method
            String className = serializedLambda.getImplClass();
            String methodName = serializedLambda.getImplMethodName();
            String methodSig = serializedLambda.getImplMethodSignature();

            Class<?> implClass = Class.forName(className.replace('/', '.'), true, Thread.currentThread().getContextClassLoader());

            // find constructor
            if (methodName.equals("<init>")) {
                Constructor<?>[] constructors = implClass.getDeclaredConstructors();
                for (Constructor<?> constructor : constructors) {
                    if (getConstructorDescriptor(constructor).equals(methodSig)) {
                        return new LambdaExecutable(constructor);
                    }
                }
            }
            // find method
            else {
                List<Method> methods = getAllDeclaredMethods(implClass);
                for (Method method : methods) {
                    if (method.getName().equals(methodName) && getMethodDescriptor(method).equals(methodSig)) {
                        return new LambdaExecutable(method);
                    }
                }
            }
            throw new TypeExtractionException("No lambda method found.");
        }
        catch (Exception e) {
            throw new TypeExtractionException("Could not extract lambda method out of function: " +
                    e.getClass().getSimpleName() + " - " + e.getMessage(), e);
        }
    }

    public static Type extractTypeFromLambda(
            Class<?> baseClass,
            LambdaExecutable exec,
            int[] lambdaTypeArgumentIndices,
            int paramLen,
            int baseParametersLen) {
        Type output = exec.getParameterTypes()[paramLen - baseParametersLen + lambdaTypeArgumentIndices[0]];
        for (int i = 1; i < lambdaTypeArgumentIndices.length; i++) {
            validateLambdaType(baseClass, output);
            output = extractTypeArgument(output, lambdaTypeArgumentIndices[i]);
        }
        validateLambdaType(baseClass, output);
        return output;
    }
    public static Type extractTypeArgument(Type t, int index) throws InvalidTypesException {
        if (t instanceof ParameterizedType) {
            Type[] actualTypeArguments = ((ParameterizedType) t).getActualTypeArguments();

            if (index < 0 || index >= actualTypeArguments.length) {
                throw new InvalidTypesException("Cannot extract the type argument with index " +
                        index + " because the type has only " + actualTypeArguments.length +
                        " type arguments.");
            } else {
                return actualTypeArguments[index];
            }
        } else {
            throw new InvalidTypesException("The given type " + t + " is not a parameterized type.");
        }
    }
    public static Method getSingleAbstractMethod(Class<?> baseClass) {

        if (!baseClass.isInterface()) {
            throw new InvalidTypesException("Given class: " + baseClass + "is not a FunctionalInterface.");
        }

        Method sam = null;
        for (Method method : baseClass.getMethods()) {
            if (Modifier.isAbstract(method.getModifiers())) {
                if (sam == null) {
                    sam = method;
                } else {
                    throw new InvalidTypesException("Given class: " + baseClass +
                            " is not a FunctionalInterface. It has more than one abstract method.");
                }
            }
        }

        if (sam == null) {
            throw new InvalidTypesException(
                    "Given class: " + baseClass + " is not a FunctionalInterface. It does not have any abstract methods.");
        }

        return sam;
    }

    /**
     * Returns all declared methods of a class including methods of superclasses.
     */
    public static List<Method> getAllDeclaredMethods(Class<?> clazz) {
        List<Method> result = new ArrayList<>();
        while (clazz != null) {
            Method[] methods = clazz.getDeclaredMethods();
            Collections.addAll(result, methods);
            clazz = clazz.getSuperclass();
        }
        return result;
    }

    /**
     * Convert ParameterizedType or Class to a Class.
     */
    public static Class<?> typeToClass(Type t) {
        if (t instanceof Class) {
            return (Class<?>)t;
        }
        else if (t instanceof ParameterizedType) {
            return ((Class<?>) ((ParameterizedType) t).getRawType());
        }
        throw new IllegalArgumentException("Cannot convert type to class");
    }

    /**
     * Checks if a type can be converted to a Class. This is true for ParameterizedType and Class.
     */
    public static boolean isClassType(Type t) {
        return t instanceof Class<?> || t instanceof ParameterizedType;
    }

    /**
     * Checks whether two types are type variables describing the same.
     */
    public static boolean sameTypeVars(Type t1, Type t2) {
        return t1 instanceof TypeVariable &&
                t2 instanceof TypeVariable &&
                ((TypeVariable<?>) t1).getName().equals(((TypeVariable<?>) t2).getName()) &&
                ((TypeVariable<?>) t1).getGenericDeclaration().equals(((TypeVariable<?>) t2).getGenericDeclaration());
    }

    /**
     * Traverses the type hierarchy of a type up until a certain stop class is found.
     *
     * @param t type for which a hierarchy need to be created
     * @return type of the immediate child of the stop class
     */
    public static Type getTypeHierarchy(List<Type> typeHierarchy, Type t, Class<?> stopAtClass) {
        while (!(isClassType(t) && typeToClass(t).equals(stopAtClass))) {
            typeHierarchy.add(t);
            t = typeToClass(t).getGenericSuperclass();

            if (t == null) {
                break;
            }
        }
        return t;
    }

    /**
     * Returns true if the given class has a superclass of given name.
     *
     * @param clazz class to be analyzed
     * @param superClassName class name of the super class
     */
    public static boolean hasSuperclass(Class<?> clazz, String superClassName) {
        List<Type> hierarchy = new ArrayList<>();
        getTypeHierarchy(hierarchy, clazz, Object.class);
        for (Type t : hierarchy) {
            if (isClassType(t) && typeToClass(t).getName().equals(superClassName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the raw class of both parameterized types and generic arrays.
     * Returns java.lang.Object for all other types.
     */
    public static Class<?> getRawClass(Type t) {
        if (isClassType(t)) {
            return typeToClass(t);
        } else if (t instanceof GenericArrayType) {
            Type component = ((GenericArrayType) t).getGenericComponentType();
            return Array.newInstance(getRawClass(component), 0).getClass();
        }
        return Object.class;
    }

    /**
     * Checks whether the given type has the generic parameters declared in the class definition.
     *
     * @param t type to be validated
     */
    public static void validateLambdaType(Class<?> baseClass, Type t) {
        if (!(t instanceof Class)) {
            return;
        }
        final Class<?> clazz = (Class<?>) t;

        if (clazz.getTypeParameters().length > 0) {
            throw new InvalidTypesException("The generic type parameters of '" + clazz.getSimpleName() + "' are missing. "
                    + "In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved. "
                    + "An easy workaround is to use an (anonymous) class instead that implements the '" + baseClass.getName() + "' interface. "
                    + "Otherwise the type has to be specified explicitly using type information.");
        }
    }


}
