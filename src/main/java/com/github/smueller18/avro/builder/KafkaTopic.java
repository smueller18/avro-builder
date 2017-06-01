package com.github.smueller18.avro.builder;

import java.lang.annotation.*;

/**
 * Copyright 2017 Stephan Müller
 * License: MIT
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KafkaTopic {
    String value();
}