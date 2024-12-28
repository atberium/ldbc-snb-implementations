package com.jackwaudby.ldbcimplementations.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;

import java.io.InputStream;
import java.util.Properties;

import static java.lang.Integer.parseInt;

/**
 * Reads in configuration properties
 */
@Slf4j
@UtilityClass
public class ImplementationConfiguration {
    private static final Properties implementationProperties = new Properties();


    static {
        try {
            final InputStream in = Configuration.class.getResourceAsStream("/implementation-configuration.properties");
            implementationProperties.load(in);
        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }


    /**
     * Get transaction retry attempts.
     *
     * @return transaction attempts
     */
    public static Integer getTxnAttempts() {
        return parseInt(implementationProperties.getProperty("txn.attempts"));
    }
}
