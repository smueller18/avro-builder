package de.stephanmueller.avro.builder;

import org.apache.avro.Schema;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
class KeyValueSchema {

    private Schema keySchema;
    private Schema valueSchema;

    KeyValueSchema(Schema keySchema, Schema valueSchema) {

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    Schema getKeySchema() {
        return keySchema;
    }

    Schema getValueSchema() {
        return valueSchema;
    }

}
