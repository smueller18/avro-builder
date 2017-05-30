package com.github.smueller18.avro.builder;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ClassUtils;

import java.lang.reflect.Field;
import java.util.HashMap;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */


public abstract class AvroBuilder {

    private static HashMap<String, KeyValueSchema> classSchemas = new HashMap<>();

    public AvroBuilder() {

        if(!classSchemas.containsKey(this.getClass().getName()))
            classSchemas.put(
                    this.getClass().getName(),
                    new KeyValueSchema(
                            this.buildSchema(true),
                            this.buildSchema(false)
                    )
            );
    }

    public final Schema getKeySchema() {
        return classSchemas.get(this.getClass().getName()).getKeySchema();
    }

    public final Schema getValueSchema() {
        return classSchemas.get(this.getClass().getName()).getValueSchema();
    }

    public final GenericRecord generateKeyRecord() {
        return generateRecord(true);
    }

    public final GenericRecord generateValueRecord() {
        return generateRecord(false);
    }

    private GenericRecord generateRecord(boolean isKey) {

        GenericRecord record;

        if (isKey)
            record = new GenericData.Record(getKeySchema());
        else
            record = new GenericData.Record(getValueSchema());

        Class keyOrValueField;
        if (isKey)
            keyOrValueField = KeyField.class;
        else
            keyOrValueField = ValueField.class;

        Class currentClass = getClass();
        while (currentClass != Object.class) {

            for(Field field : currentClass.getDeclaredFields()) {

                if (field.getAnnotation(keyOrValueField) != null) {

                    // NOTE: support for nested types not checked!
                    try {
                        field.setAccessible(true);
                        record.put(field.getName(), field.get(this));
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }

            currentClass = currentClass.getSuperclass();
        }

        return record;

    }

    private String getRecordName(boolean isKey) {
        String recordName;
        if (getClass().getAnnotation(Name.class) != null)
            recordName = getClass().getAnnotation(Name.class).value();
        else
            recordName = getClass().getName().replace("$", ".");

        if(isKey)
            return recordName + "_key";
        else
            return recordName + "_value";
    }

    private String getNamespace() {
        if (getClass().getAnnotation(Namespace.class) != null)
            return getClass().getAnnotation(Namespace.class).value();
        else
            return ClassUtils.getPackageName(getClass());
    }

    @SuppressWarnings("unchecked")
    private Schema buildSchema(boolean isKey) {

        SchemaBuilder.RecordBuilder recordBuilder = SchemaBuilder
                .record(getRecordName(isKey))
                .namespace(getNamespace());

        if (getClass().getAnnotation(Doc.class) != null)
            recordBuilder.doc(getClass().getAnnotation(Doc.class).value());

        SchemaBuilder.FieldAssembler fieldAssembler = recordBuilder.fields();

        Class currentClass = getClass();
        while (currentClass != Object.class) {

            for(Field field : currentClass.getDeclaredFields()) {

                if (field.getAnnotation((Class) ((isKey) ? KeyField.class : ValueField.class)) != null) {

                    SchemaBuilder.FieldBuilder fieldBuilder = fieldAssembler.name(
                            getClass().getAnnotation(Name.class) != null ?
                                    getClass().getAnnotation(Name.class).value() :
                                    field.getName()
                    );

                    if (field.getAnnotation(Doc.class) != null) {
                        fieldBuilder.doc(field.getAnnotation(Doc.class).value());
                    }

                    switch (FieldTypeUtils.fieldTypeToSchemaType(field)) {

                        case BOOLEAN:
                            fieldBuilder.type().booleanType().noDefault();
                            break;
                        case INT:
                            fieldBuilder.type().intType().noDefault();
                            break;
                        case LONG:
                            fieldBuilder.type().longType().noDefault();
                            break;
                        case FLOAT:
                            fieldBuilder.type().floatType().noDefault();
                            break;
                        case DOUBLE:
                            fieldBuilder.type().doubleType().noDefault();
                            break;
                        case BYTES:
                            fieldBuilder.type().bytesType().noDefault();
                            break;
                        case STRING:
                            fieldBuilder.type().stringType().noDefault();
                            break;
                        case TIMESTAMP_MILLIS:
                            Schema timestampMillisType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                            fieldBuilder.type(timestampMillisType).noDefault();
                            break;
                        case NULLABLE_INT:
                            fieldBuilder.type().nullable().intType().noDefault();
                            break;
                        case NULLABLE_LONG:
                            fieldBuilder.type().nullable().longType().noDefault();
                            break;
                        case NULLABLE_FLOAT:
                            fieldBuilder.type().nullable().floatType().noDefault();
                            break;
                        case NULLABLE_DOUBLE:
                            fieldBuilder.type().nullable().doubleType().noDefault();
                            break;
                        case NULLABLE_BYTES:
                            fieldBuilder.type().nullable().bytesType().noDefault();
                            break;
                        case NULLABLE_STRING:
                            fieldBuilder.type().nullable().stringType().noDefault();
                            break;

                        case NULL:
                            fieldBuilder.type().nullType().noDefault();
                            break;
                    }
                }
            }
            currentClass = currentClass.getSuperclass();
        }

        return (Schema) fieldAssembler.endRecord();
    }

    public String toString() {
        return String.format("Key schema: %s \nValue schema: %s", getKeySchema(), getValueSchema());
    }
}
