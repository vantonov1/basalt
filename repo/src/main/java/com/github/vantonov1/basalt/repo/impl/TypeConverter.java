package com.github.vantonov1.basalt.repo.impl;

import org.springframework.util.SerializationUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;

public class TypeConverter {
    public static String NULL_STRING = "NULL STRING";
    public static String NULL_NUMERIC = "NULL NUMERIC";

    private enum TYPES {
        UNKNOWN, INT, LONG, FLOAT, DOUBLE, STRING, DATE, BOOLEAN, SERIALIZABLE
    }

    public static int getType(Object value) {
        if (value == null) {
            return TYPES.UNKNOWN.ordinal();
        } else if (value instanceof Integer) {
            return TYPES.INT.ordinal();
        } else if (value instanceof Long) {
            return TYPES.LONG.ordinal();
        } else if (value instanceof Double || value instanceof Float) {
            return TYPES.DOUBLE.ordinal();
        } else if (value instanceof Date) {
            return TYPES.DATE.ordinal();
        } else if (value instanceof String) {
            return TYPES.STRING.ordinal();
        } else if (value instanceof Boolean) {
            return TYPES.BOOLEAN.ordinal();
        } else if (value instanceof Serializable) {
            return TYPES.SERIALIZABLE.ordinal();
        }
        throw new RuntimeException("unknown type" + value.getClass());
    }

    public static Serializable convert(int type, String v_string, long v_numeric) {
        int valueType = Math.abs(type);
        if (valueType == TYPES.UNKNOWN.ordinal()) {
            return null;
        } else if (valueType == TYPES.INT.ordinal()) {
            return ((int) v_numeric);
        } else if (valueType == TYPES.LONG.ordinal()) {
            return v_numeric;
        } else if (valueType == TYPES.DOUBLE.ordinal()) {
            return Double.longBitsToDouble(v_numeric);
        } else if (valueType == TYPES.FLOAT.ordinal()) {
            return Float.intBitsToFloat((int) v_numeric);
        } else if (valueType == TYPES.DATE.ordinal()) {
            return new Date(v_numeric);
        } else if (valueType == TYPES.BOOLEAN.ordinal()) {
            return Boolean.valueOf(v_string);
        } else if (valueType == TYPES.STRING.ordinal()) {
            return v_string;
        } else if (valueType == TYPES.SERIALIZABLE.ordinal()) {
            final byte[] bytes = Base64.getDecoder().decode(v_string.getBytes(StandardCharsets.UTF_8));
            return (Serializable) SerializationUtils.deserialize(bytes);
        }
        throw new RuntimeException("unknown type");
    }

    public static String getString(Object v) {
        int valueType = getType(v);
        if (valueType == TYPES.BOOLEAN.ordinal()) {
            assert v instanceof Boolean;
            return Boolean.TRUE.equals(v) ? "true" : "false";
        } else if (valueType == TYPES.STRING.ordinal()) {
            assert v instanceof String;
            return (String) v;
        } else if (valueType == TYPES.SERIALIZABLE.ordinal()) {
            final byte[] bytes = Base64.getEncoder().encode(SerializationUtils.serialize(v));
            return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
        }
        return null;
    }

    public static Long getNumeric(Object v) {
        int valueType = getType(v);
        if (valueType == TYPES.INT.ordinal()) {
            assert v instanceof Integer;
            return (long) ((Integer) v);
        } else if (valueType == TYPES.LONG.ordinal()) {
            assert v instanceof Long;
            return (long) v;
        } else if (valueType == TYPES.FLOAT.ordinal()) {
            assert v instanceof Float;
            return (long) Float.floatToIntBits((Float) v);
        } else if (valueType == TYPES.DOUBLE.ordinal()) {
            assert v instanceof Double;
            return Double.doubleToLongBits((Double) v);
        } else if (valueType == TYPES.DATE.ordinal()) {
            assert v instanceof Date;
            return ((Date) v).getTime();
        }
        return null;
    }
}