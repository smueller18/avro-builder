package com.github.smueller18.avro.builder;

import org.apache.avro.Schema;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
public enum SchemaType {
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BYTES,
    STRING,
    TIMESTAMP_MILLIS,
    NULLABLE_INT,
    NULLABLE_LONG,
    NULLABLE_FLOAT,
    NULLABLE_DOUBLE,
    NULLABLE_BYTES,
    NULLABLE_STRING,
    NULLABLE_BOOLEAN,
    NULL;

    public static Schema.Type SchemaTypeToAvroSchemaType(SchemaType schemaType) {
        switch (schemaType) {
            case BOOLEAN:
            case NULLABLE_BOOLEAN:
                return Schema.Type.BOOLEAN;
            case INT:
            case NULLABLE_INT:
                return Schema.Type.INT;
            case LONG:
            case NULLABLE_LONG:
            case TIMESTAMP_MILLIS:
                return Schema.Type.LONG;
            case FLOAT:
            case NULLABLE_FLOAT:
                return Schema.Type.FLOAT;
            case DOUBLE:
            case NULLABLE_DOUBLE:
                return Schema.Type.DOUBLE;
            case BYTES:
            case NULLABLE_BYTES:
                return Schema.Type.BYTES;
            case STRING:
            case NULLABLE_STRING:
                return Schema.Type.STRING;
            case NULL:
            default:
                return Schema.Type.NULL;
        }
    }

    public static SchemaType fieldTypeToSchemaType(Field field) throws ClassCastException {

        String fieldTypeName;

        if (field.getAnnotation(TimestampMillisType.class) != null) {

            if(!(field.getType().getName().equals("long") || field.getType().getName().equals("class java.lang.Long")))
                throw new ClassCastException(
                        String.format("the type of field '%s' must be long because it is marked as TimestampMillisType", field.getName())
                );

            return SchemaType.TIMESTAMP_MILLIS;
        }

        if(field.getType().getName().equals("class java.util.ArrayList"))
            fieldTypeName = ((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0].getTypeName();
        else
            fieldTypeName = field.getType().getName();

        switch (fieldTypeName) {

            case "boolean":
                return SchemaType.BOOLEAN;

            case "java.lang.Boolean":
                return SchemaType.NULLABLE_BOOLEAN;

            case "int":
                return SchemaType.INT;

            case "java.lang.Integer":
                return SchemaType.NULLABLE_INT;

            case "long":
                return SchemaType.LONG;

            case "java.lang.Long":
                return SchemaType.NULLABLE_LONG;

            case "float":
                return SchemaType.FLOAT;

            case "java.lang.Float":
                return SchemaType.NULLABLE_FLOAT;

            case "double":
                return SchemaType.DOUBLE;

            case "java.lang.Double":
                return SchemaType.NULLABLE_DOUBLE;

            case "bytes":
                return SchemaType.BYTES;

            case "java.lang.Bytes":
                return SchemaType.NULLABLE_BYTES;

            case "java.lang.String":
                if (field.getAnnotation(TimestampMillisType.class) != null)
                    return SchemaType.NULLABLE_STRING;
                return SchemaType.STRING;

            default:
                return SchemaType.NULL;
        }
    }
}
