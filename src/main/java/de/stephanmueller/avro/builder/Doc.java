package de.stephanmueller.avro.builder;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */

@Retention(RetentionPolicy.RUNTIME)
public @interface Doc {
    String value();
}