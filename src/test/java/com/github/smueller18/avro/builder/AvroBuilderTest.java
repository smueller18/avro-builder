package com.github.smueller18.avro.builder;

import org.junit.jupiter.api.Test;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
class AvroBuilderTest {

    private static final String recordName = "record_name";

    @Test
    void Test() {

        TestClass firstTest = new TestClass(
                1, 1.0, null, true,
                "string", "customNameString", null
        );

        assert firstTest.getKeySchema() != null;
        assert firstTest.getValueSchema() != null;

        System.out.println("First test schemas: " + firstTest);
        System.out.println("First test key record: " + firstTest.generateKeyRecord());
        System.out.println("First test value record: " + firstTest.generateValueRecord());

        TestClass secondTest = new TestClass(
                1, 1.0, 5.0, true,
                "string", "string", "string"
        );

        assert secondTest.getKeySchema() != null;
        assert secondTest.getValueSchema() != null;

        System.out.println("Second test schemas: " + secondTest);
        System.out.println("Second test key record: " + secondTest.generateKeyRecord());
        System.out.println("Second test value record: " + secondTest.generateValueRecord());
    }

    @Documentation("This is the description of the avro record")
    @Name(recordName)
    class TestClass extends AvroBuilder {

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

        TestClass(long timestamp, double doubleValue, Double nullableDoubleValue, boolean booleanValue,
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

}