package com.levyx.xenon.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Returns Version of the connector being developed.
 */
public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String VERSION;


    static {
        String versionProperty = "unknown";
        try {
            Properties props = new Properties();
            props.load(Version.class.getResourceAsStream("/kafka-connect-xenon-version"
                    + ".properties"));
            versionProperty = props.getProperty("version", versionProperty).trim();
        } catch (IOException e) {
            e.printStackTrace();
            versionProperty = "unknown";
        }
        VERSION = versionProperty;
    }

    public static String getVersion() {
        return VERSION;
    }
}
