package com.github.smueller18.avro.builder;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
class AvroBuilderTest {

    private static final String topicNamespace = "namepsace";
    private static final String topicName = "topic";

    @Test
    void full() {

        Full firstTest = new Full(
                1, 1.0, null, true,
                "string", "customNameString", null
        );

        assert firstTest.getKeySchema() != null;
        assert firstTest.getValueSchema() != null;

        System.out.println("First test schemas: " + firstTest);
        System.out.println("First test key record: " + firstTest.getKeyRecord());
        System.out.println("First test value record: " + firstTest.getValueRecord());

        Full secondTest = new Full(
                1, 1.0, 5.0, true,
                "string", "string", "string"
        );

        assert secondTest.getKeySchema() != null;
        assert secondTest.getValueSchema() != null;

        System.out.println("Second test schemas: " + secondTest);
        System.out.println("Second test key record: " + secondTest.getKeyRecord());
        System.out.println("Second test value record: " + secondTest.getValueRecord());
    }

    @Test
    void noTopicDefined() {
        assertThrows(RuntimeException.class, () -> {
            new NoTopicDefined();
        });
    }

    @Test
    void dotsInTopic() {
        assertThrows(RuntimeException.class, () -> {
            new DotsInTopicName();
        });
    }

    @Test
    void getTopic() {
        assert AvroBuilder.getTopicName(Empty.class).equals("namespace.topic_name");
    }

    @Test
    void arrayList() {
        Array array = new Array(1, new ArrayList<>(Arrays.asList(1.0, 2.0, 3.2)), new ArrayList<>(Arrays.asList(1, 2, 3)));

        System.out.println("ArrayList test schemas: " + array);
        System.out.println("ArrayList test key record: " + array.getKeyRecord());
        System.out.println("ArrayList test value record: " + array.getValueRecord());
    }


    @Documentation("This is the description of the avro record")
    @KafkaTopic(namespace = topicNamespace, name = topicName)
    private class Full extends AvroBuilder {

        @Key
        @TimestampMillisType
        @Documentation("timestamp-millis value")
        private long timestamp = 0;

        @Value
        @Documentation("not nullable double value")
        private double doubleValue = 0;

        @Value
        @Documentation("nullable double value")
        private Double nullableDoubleValue = null;

        @Value
        @Documentation("boolean value")
        private boolean booleanValue;

        @Value
        @Documentation("string value which cannot be null")
        private String stringValue;

        @Value
        @Name("String_Value_With_Custom_Name")
        @Documentation("string value which cannot be null")
        private String stringValueWithCustomName;

        @Value
        @Nullable
        @Documentation("string value which can be null")
        private String nullableStringValue;

        Full(long timestamp, double doubleValue, Double nullableDoubleValue, boolean booleanValue,
             String stringValue, String stringValueWithCustomName, String nullableStringValue) {
            this.timestamp = timestamp;
            this.doubleValue = doubleValue;
            this.nullableDoubleValue = nullableDoubleValue;
            this.booleanValue = booleanValue;
            this.stringValue = stringValue;
            this.stringValueWithCustomName = stringValueWithCustomName;
            this.nullableStringValue = nullableStringValue;
        }
    }

    @KafkaTopic(namespace = "namespace", name = "topic_name")
    private class Empty extends AvroBuilder {

    }

    @Documentation("Class will fail because KafkaTopic is not defined")
    private class NoTopicDefined extends AvroBuilder {

    }

    @Documentation("Class initialization will fail because name of KafkaTopic contains dots")
    @KafkaTopic(namespace = "namespace", name = "topic.name")
    private class DotsInTopicName extends AvroBuilder {

    }

    @Documentation("Example for an ArrayList")
    @KafkaTopic(namespace = "namespace", name = "aray_list")
    private class Array extends AvroBuilder {

        @Key
        @TimestampMillisType
        @Documentation("timestamp-millis value")
        private long timestamp = 0;

        @Value
        @Documentation("double ArrayList")
        private ArrayList<Double> doubleArray = null;

        @Value
        @Documentation("integer ArrayList")
        private ArrayList<Integer> intArray = null;

        Array(long timestamp, ArrayList<Double> doubleArray, ArrayList<Integer> intArray) {
            this.timestamp = timestamp;
            this.doubleArray = doubleArray;
            this.intArray = intArray;
        }
    }


}