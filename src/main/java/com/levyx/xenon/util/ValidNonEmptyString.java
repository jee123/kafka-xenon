package com.levyx.xenon.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * {{@link ValidNonEmptyString}.java
 * Purpose: Validate the Configuration definition for the Connector
 *     ensuring user is aware of empty String passed.
 */
public class ValidNonEmptyString implements ConfigDef.Validator {

    /**
     * Method to validate configuration.
     *
     * @param name - Connector configuration name.
     * @param value - Value to be validated.
     */
    @Override
    public void ensureValid(String name, Object value) {
        if (value instanceof String) {
            if (((String) value).trim().isEmpty()) {
                throw new ConfigException(name, value, "String should be empty "
                        + "only if DEFAULT values acceptable.");
            }
        }
    }
}