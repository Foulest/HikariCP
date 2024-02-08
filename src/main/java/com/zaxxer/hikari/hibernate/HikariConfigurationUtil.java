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
    public static final String CONFIG_PREFIX_DATASOURCE = "hibernate.hikari.dataSource.";

    /**
     * Create/load a HikariConfig from Hibernate properties.
     *
     * @param props a map of Hibernate properties
     * @return a HikariConfig
     */
    @Contract("_ -> new")
    @SuppressWarnings("rawtypes")
    public static @NotNull HikariConfig loadConfiguration(Map props) {
        Properties hikariProps = new Properties();
        copyProperty(AvailableSettings.ISOLATION, props, "transactionIsolation", hikariProps);
        copyProperty(AvailableSettings.AUTOCOMMIT, props, "autoCommit", hikariProps);
        copyProperty(AvailableSettings.DRIVER, props, "driverClassName", hikariProps);
        copyProperty(AvailableSettings.URL, props, "jdbcUrl", hikariProps);
        copyProperty(AvailableSettings.USER, props, "username", hikariProps);
        copyProperty(AvailableSettings.PASS, props, "password", hikariProps);

        for (Object keyo : props.keySet()) {
            String key = (String) keyo;

            if (key.startsWith(CONFIG_PREFIX)) {
                hikariProps.setProperty(key.substring(CONFIG_PREFIX.length()), (String) props.get(key));
            }
        }
        return new HikariConfig(hikariProps);
    }

    @SuppressWarnings("rawtypes")
    private static void copyProperty(String srcKey, @NotNull Map src, String dstKey, Properties dst) {
        if (src.containsKey(srcKey)) {
            dst.setProperty(dstKey, (String) src.get(srcKey));
        }
    }
}
