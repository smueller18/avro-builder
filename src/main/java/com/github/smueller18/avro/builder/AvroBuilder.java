package com.github.smueller18.avro.builder;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Field;
import java.util.HashMap;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */

public abstract class AvroBuilder {

    private static HashMap<String, KeyValueSchema> classSchemas = new HashMap<>();

    public AvroBuilder() throws RuntimeException {

        validateKafkaTopic(this.getClass());

        if(!classSchemas.containsKey(this.getClass().getName()))
            classSchemas.put(
                    this.getClass().getName(),
                    new KeyValueSchema(
                            this.buildSchema(true),
                            this.buildSchema(false)
                    )
            );
    }

    private static boolean validateKafkaTopic(Class<? extends AvroBuilder> cla) throws RuntimeException {

        if (cla.getAnnotation(KafkaTopic.class) != null) {
            if (cla.getAnnotation(KafkaTopic.class).name().contains("."))
                throw new RuntimeException(
                        String.format("The name parameter of the annotation @KafkaTopic for class %s must not contain dots",
                                cla.getName()
                        )
                );
        }
        else {
            throw new RuntimeException(
                    String.format("Annotation @KafkaTopic has to be defined for class %s", cla.getName())
            );
        }

        return true;
    }

    public final Schema getKeySchema() {
        return classSchemas.get(this.getClass().getName()).getKeySchema();
    }

    public final Schema getValueSchema() {
        return classSchemas.get(this.getClass().getName()).getValueSchema();
    }

    public final GenericRecord getKeyRecord() {
        return getRecord(true);
    }

    public final GenericRecord getValueRecord() {
        return getRecord(false);
    }

    @SuppressWarnings("unchecked")
    private GenericRecord getRecord(boolean isKey) {

        GenericRecord record;

        if (isKey)
            record = new GenericData.Record(getKeySchema());
        else
            record = new GenericData.Record(getValueSchema());

        Class currentClass = getClass();
        while (currentClass != Object.class) {

            for(Field field : currentClass.getDeclaredFields()) {

                if (field.getAnnotation((Class) ((isKey) ? Key.class : Value.class)) != null) {

                    // NOTE: support for nested types not checked!
                    try {
                        field.setAccessible(true);
                        record.put(getFieldName(field), field.get(this));
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }

            currentClass = currentClass.getSuperclass();
        }

        return record;

    }

    private static String getFieldName(Field field) {
        if(field.getAnnotation(Name.class) != null)
            return field.getAnnotation(Name.class).value();
        return field.getName();
    }

    private static String getNamespace(Class<? extends AvroBuilder> cla) {
        validateKafkaTopic(cla);
        return cla.getAnnotation(KafkaTopic.class).namespace();
    }

    private static String getName(Class<? extends AvroBuilder> cla) {
        validateKafkaTopic(cla);
        return cla.getAnnotation(KafkaTopic.class).name();
    }

    public static String getTopicName(Class<? extends AvroBuilder> cla) throws RuntimeException {
        validateKafkaTopic(cla);
        return cla.getAnnotation(KafkaTopic.class).namespace() + "." + cla.getAnnotation(KafkaTopic.class).name();
    }

    @SuppressWarnings("unchecked")
    private Schema buildSchema(boolean isKey) {

        SchemaBuilder.RecordBuilder recordBuilder = SchemaBuilder
                .record(getName(this.getClass()))
                .namespace(getNamespace(this.getClass()));

        if (getClass().getAnnotation(Documentation.class) != null)
            recordBuilder.doc(getClass().getAnnotation(Documentation.class).value());

        SchemaBuilder.FieldAssembler fieldAssembler = recordBuilder.fields();

        Class currentClass = getClass();
        while (currentClass != Object.class) {

            for(Field field : currentClass.getDeclaredFields()) {

                if (field.getAnnotation((Class) ((isKey) ? Key.class : Value.class)) != null) {

                    SchemaBuilder.FieldBuilder fieldBuilder = fieldAssembler.name(getFieldName(field));

                    if (field.getAnnotation(Documentation.class) != null) {
                        fieldBuilder.doc(field.getAnnotation(Documentation.class).value());
                    }

                    // handle ArrayLists
                    if(field.getType().getName().equals("class java.util.ArrayList"))
                        fieldBuilder.type(Schema.createArray(Schema.create(
                                SchemaType.SchemaTypeToAvroSchemaType(SchemaType.fieldTypeToSchemaType(field))
                        ))).noDefault();

                    // handle other data types
                    else
                        switch (SchemaType.fieldTypeToSchemaType(field)) {

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
