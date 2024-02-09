package com.zaxxer.hikari.hibernate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.hibernate.HibernateException;
import org.hibernate.Version;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.UnknownUnwrapTypeException;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Stoppable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Connection provider for Hibernate 4.3.
 *
 * @author Brett Wooldridge, Luca Burgazzoli
 */
@SuppressWarnings("unused")
public class HikariConnectionProvider implements ConnectionProvider, Configurable, Stoppable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HikariConnectionProvider.class);
    private static final long serialVersionUID = -9131625057941275711L;

    private HikariConfig config;
    private HikariDataSource dataSource;

    public HikariConnectionProvider() {
        config = null;
        dataSource = null;

        if (Version.getVersionString().substring(0, 5).compareTo("4.3.6") >= 1) {
            LOGGER.warn("com.zaxxer.hikari.hibernate.HikariConnectionProvider has"
                    + " been deprecated for versions of Hibernate 4.3.6 and newer."
                    + " Please switch to org.hibernate.hikaricp.internal.HikariCPConnectionProvider.");
        }
    }

    // *************************************************************************
    // Configurable
    // *************************************************************************

    @Override
    public void configure(@NotNull Map props) throws HibernateException {
        try {
            LOGGER.debug("Configuring HikariCP");
            config = HikariConfigurationUtil.loadConfiguration(props);
            dataSource = new HikariDataSource(config);
        } catch (Exception ex) {
            throw new HibernateException(ex);
        }
        LOGGER.debug("HikariCP Configured");
    }

    // *************************************************************************
    // ConnectionProvider
    // *************************************************************************

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = null;

        if (dataSource != null) {
            connection = dataSource.getConnection();
        }
        return connection;
    }

    @Override
    public void closeConnection(@NotNull Connection connection) throws SQLException {
        connection.close();
    }

    @Override
    public boolean supportsAggressiveRelease() {
        return false;
    }

    @Override
    public boolean isUnwrappableAs(@NotNull Class unwrapType) {
        return ConnectionProvider.class.equals(unwrapType)
                || HikariConnectionProvider.class.isAssignableFrom(unwrapType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(@NotNull Class<T> unwrapType) {
        if (ConnectionProvider.class.equals(unwrapType)
                || HikariConnectionProvider.class.isAssignableFrom(unwrapType)) {
            return (T) this;
        } else if (DataSource.class.isAssignableFrom(unwrapType)) {
            return (T) dataSource;
        } else {
            throw new UnknownUnwrapTypeException(unwrapType);
        }
    }

    // *************************************************************************
    // Stoppable
    // *************************************************************************

    @Override
    public void stop() {
        dataSource.close();
    }
}
