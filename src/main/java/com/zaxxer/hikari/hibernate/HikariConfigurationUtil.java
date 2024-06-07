/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Modifications made by Foulest (https://github.com/Foulest)
 * for the HikariCP fork (https://github.com/Foulest/HikariCP).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
