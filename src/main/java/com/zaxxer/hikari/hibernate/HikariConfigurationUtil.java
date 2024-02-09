package com.zaxxer.hikari.hibernate;

import com.zaxxer.hikari.HikariConfig;
import org.hibernate.cfg.AvailableSettings;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Properties;

/**
 * Utility class to map Hibernate properties to HikariCP configuration properties.
 *
 * @author Brett Wooldridge, Luca Burgazzoli
 */
public class HikariConfigurationUtil {

    public static final String CONFIG_PREFIX = "hibernate.hikari.";

    /**
     * Create/load a HikariConfig from Hibernate properties.
     *
     * @param props a map of Hibernate properties
     * @return a HikariConfig
     */
    @Contract("_ -> new")
    @SuppressWarnings("rawtypes")
    public static @NotNull HikariConfig loadConfiguration(Map props) {
        Properties properties = new Properties();
        copyProperty(AvailableSettings.ISOLATION, props, "transactionIsolation", properties);
        copyProperty(AvailableSettings.AUTOCOMMIT, props, "autoCommit", properties);
        copyProperty(AvailableSettings.DRIVER, props, "driverClassName", properties);
        copyProperty(AvailableSettings.URL, props, "jdbcUrl", properties);
        copyProperty(AvailableSettings.USER, props, "username", properties);
        copyProperty(AvailableSettings.PASS, props, "password", properties);

        for (Object keyo : props.keySet()) {
            String key = (String) keyo;

            if (key.startsWith(CONFIG_PREFIX)) {
                properties.setProperty(key.substring(CONFIG_PREFIX.length()), (String) props.get(key));
            }
        }
        return new HikariConfig(properties);
    }

    @SuppressWarnings("rawtypes")
    private static void copyProperty(String srcKey, @NotNull Map src, String dstKey, Properties dst) {
        if (src.containsKey(srcKey)) {
            dst.setProperty(dstKey, (String) src.get(srcKey));
        }
    }
}
