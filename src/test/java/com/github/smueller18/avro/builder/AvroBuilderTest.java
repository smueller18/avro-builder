package com.github.smueller18.avro.builder;

import org.junit.jupiter.api.Test;
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

}