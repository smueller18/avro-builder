package com.github.smueller18.avro.builder;

import org.junit.jupiter.api.Test;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
class AvroBuilderTest {

    @Test
    void AvroBuilderTest() {

        TestClass firstTest = new TestClass(1, 1.0, null, true, "string", null);

        assert firstTest.getKeySchema() != null;
        assert firstTest.getValueSchema() != null;

        System.out.println("First test schemas: " + firstTest);
        System.out.println("First test key record: " + firstTest.generateKeyRecord());
        System.out.println("First test value record: " + firstTest.generateValueRecord());

        TestClass secondTest = new TestClass(1, 1.0, 5.0, true, "string", "string");

        assert secondTest.getKeySchema() != null;
        assert secondTest.getValueSchema() != null;

        System.out.println("Second test schemas: " + secondTest);
        System.out.println("Second test key record: " + secondTest.generateKeyRecord());
        System.out.println("Second test value record: " + secondTest.generateValueRecord());
    }

    @Doc("This is the description of the avro record")
    class TestClass extends AvroBuilder {

        @KeyField
        @TimestampMillisType
        @Doc("timestamp-millis value")
        private long timestamp = 0;

        @ValueField
        @Doc("not nullable double value")
        private double doubleValue = 0;

        @ValueField
        @Doc("nullable double value")
        private Double nullableDoubleValue = null;

        @ValueField
        @Doc("boolean value")
        private boolean booleanValue;

        @ValueField
        @Doc("string value which cannot be null")
        private String stringValue;

        @ValueField
        @Nullable
        @Doc("string value which can be null")
        private String nullableStringValue;

        TestClass(long timestamp, double doubleValue, Double nullableDoubleValue, boolean booleanValue, String stringValue, String nullableStringValue) {
            this.timestamp = timestamp;
            this.doubleValue = doubleValue;
            this.nullableDoubleValue = nullableDoubleValue;
            this.booleanValue = booleanValue;
            this.stringValue = stringValue;
            this.nullableStringValue = nullableStringValue;
        }
    }

}