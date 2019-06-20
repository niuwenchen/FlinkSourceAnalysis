## BasicTypeInfo
    
    BasicTypeInfo(Class<T> clazz, Class<?>[] possibleCastTargetTypes, TypeSerializer<T> serializer, Class<? extends TypeComparator<T>> comparatorClass)
        clazz: 目标类型 sting boolean byte int等
        possibleCastTargetTypes： new Class<?>[]{}
        serializer： 类型序列化
        comparatorClass： 比较器


### 具体说明

    ****1 
    String_type_info: new BasicTypeInfo<>(String.class, new Class<?>[]{}, DefaultSerializers.StringSerializer.INSTANCE, StringComparator.class);
    
    