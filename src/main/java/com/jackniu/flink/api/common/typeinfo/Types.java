package com.jackniu.flink.api.common.typeinfo;

import com.jackniu.flink.api.java.typeutils.RowTypeInfo;
import com.jackniu.flink.types.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Created by JackNiu on 2019/7/1.
 */
public class Types {
    public static final TypeInformation<Void> VOID = BasicTypeInfo.VOID_TYPE_INFO;
    public static final TypeInformation<String> STRING = BasicTypeInfo.STRING_TYPE_INFO;
    public static final TypeInformation<Byte> BYTE = BasicTypeInfo.BYTE_TYPE_INFO;
    public static final TypeInformation<Boolean> BOOLEAN = BasicTypeInfo.BOOLEAN_TYPE_INFO;
    public static final TypeInformation<Short> SHORT = BasicTypeInfo.SHORT_TYPE_INFO;
    public static final TypeInformation<Integer> INT = BasicTypeInfo.INT_TYPE_INFO;
    public static final TypeInformation<Long> LONG = BasicTypeInfo.LONG_TYPE_INFO;
    public static final TypeInformation<Float> FLOAT = BasicTypeInfo.FLOAT_TYPE_INFO;
    public static final TypeInformation<Double> DOUBLE = BasicTypeInfo.DOUBLE_TYPE_INFO;
    public static final TypeInformation<Character> CHAR = BasicTypeInfo.CHAR_TYPE_INFO;
    public static final TypeInformation<BigDecimal> BIG_DEC = BasicTypeInfo.BIG_DEC_TYPE_INFO;
    public static final TypeInformation<BigInteger> BIG_INT = BasicTypeInfo.BIG_INT_TYPE_INFO;
    public static final TypeInformation<Date> SQL_DATE = SqlTimeTypeInfo.DATE;
    public static final TypeInformation<Time> SQL_TIME = SqlTimeTypeInfo.TIME;
    public static final TypeInformation<Timestamp> SQL_TIMESTAMP = SqlTimeTypeInfo.TIMESTAMP;
    public static final TypeInformation<Instant> INSTANT = BasicTypeInfo.INSTANT_TYPE_INFO;

    public static TypeInformation<Row> ROW(TypeInformation<?>... types) {
        return new RowTypeInfo(types);
    }


}

