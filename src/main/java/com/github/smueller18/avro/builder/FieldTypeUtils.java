package com.github.smueller18.avro.builder;

import java.lang.reflect.Field;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
class FieldTypeUtils {

    static SchemaType fieldTypeToSchemaType(Field field) throws ClassCastException {

        if (field.getAnnotation(TimestampMillisType.class) != null) {

            if(!(field.getType().getName().equals("long") || field.getType().getName().equals("class java.lang.Long")))
                throw new ClassCastException(
                        String.format("the type of field '%s' must be long because it is marked as TimestampMillisType", field.getName())
                );

            return SchemaType.TIMESTAMP_MILLIS;
        }

        switch (field.getType().getName()) {

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
