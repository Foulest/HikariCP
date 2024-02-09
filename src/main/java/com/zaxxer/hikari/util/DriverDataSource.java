package com.zaxxer.hikari.util;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.Properties;

public final class DriverDataSource implements DataSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverDataSource.class);
    private static final String PASSWORD = "password";
    private static final String USER = "user";

    private final String jdbcUrl;
    private final Properties driverProperties;

    private Driver driver;

    public DriverDataSource(String jdbcUrl, String driverClassName,
                            @NotNull Properties properties, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        driverProperties = new Properties();

        for (Entry<Object, Object> entry : properties.entrySet()) {
            driverProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }

        if (username != null) {
            driverProperties.put(USER, driverProperties.getProperty(USER, username));
        }

        if (password != null) {
            driverProperties.put(PASSWORD, driverProperties.getProperty(PASSWORD, password));
        }

        if (driverClassName != null) {
            Enumeration<Driver> drivers = DriverManager.getDrivers();

            while (drivers.hasMoreElements()) {
                Driver nextElement = drivers.nextElement();

                if (nextElement.getClass().getName().equals(driverClassName)) {
                    driver = nextElement;
                    break;
                }
            }

            if (driver == null) {
                LOGGER.warn("Registered driver with driverClassName={} was not found,"
                        + " trying direct instantiation.", driverClassName);

                Class<?> driverClass = null;
                ClassLoader threadContextClassLoader = Thread.currentThread().getContextClassLoader();

                try {
                    if (threadContextClassLoader != null) {
                        try {
                            driverClass = threadContextClassLoader.loadClass(driverClassName);
                            LOGGER.debug("Driver class {} found in Thread context class loader {}",
                                    driverClassName, threadContextClassLoader);
                        } catch (ClassNotFoundException ex) {
                            LOGGER.debug("Driver class {} not found in Thread"
                                            + " context class loader {}, trying classloader {}",
                                    driverClassName, threadContextClassLoader, getClass().getClassLoader());
                        }
                    }

                    if (driverClass == null) {
                        driverClass = getClass().getClassLoader().loadClass(driverClassName);
                        LOGGER.debug("Driver class {} found in the HikariConfig class classloader {}",
                                driverClassName, getClass().getClassLoader());
                    }
                } catch (ClassNotFoundException ex) {
                    LOGGER.debug("Failed to load driver class {} from HikariConfig class classloader {}",
                            driverClassName, getClass().getClassLoader());
                }

                if (driverClass != null) {
                    try {
                        driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
                    } catch (Exception ex) {
                        LOGGER.warn("Failed to create instance of driver class {},"
                                + " trying jdbcUrl resolution", driverClassName, ex);
                    }
                }
            }
        }

        String sanitizedUrl = jdbcUrl.replaceAll("([?&;]password=)[^&#;]*(.*)", "$1<masked>$2");
        try {
            if (driver == null) {
                driver = DriverManager.getDriver(jdbcUrl);
                LOGGER.debug("Loaded driver with class name {} for jdbcUrl={}",
                        driver.getClass().getName(), sanitizedUrl);
            } else if (!driver.acceptsURL(jdbcUrl)) {
                throw new RuntimeException("Driver " + driverClassName
                        + " claims to not accept jdbcUrl, " + sanitizedUrl);
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to get driver instance for jdbcUrl=" + sanitizedUrl, ex);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(jdbcUrl, driverProperties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties cloned = (Properties) driverProperties.clone();

        if (username != null) {
            cloned.put(USER, username);

            if (cloned.containsKey("username")) {
                cloned.put("username", username);
            }
        }

        if (password != null) {
            cloned.put(PASSWORD, password);
        }
        return driver.connect(jdbcUrl, cloned);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLogWriter(PrintWriter logWriter) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
        DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return driver.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
