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
public class HikariConnectionProvider implements ConnectionProvider, Configurable, Stoppable {

    private static final long serialVersionUID = -9131625057941275711L;

    private static final Logger LOGGER = LoggerFactory.getLogger(HikariConnectionProvider.class);

    /**
     * HikariCP configuration.
     */
    private HikariConfig hcfg;

    /**
     * HikariCP data source.
     */
    private HikariDataSource hds;

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * c-tor
     */
    public HikariConnectionProvider() {
        hcfg = null;
        hds = null;

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

            hcfg = HikariConfigurationUtil.loadConfiguration(props);
            hds = new HikariDataSource(hcfg);

        } catch (Exception e) {
            throw new HibernateException(e);
        }

        LOGGER.debug("HikariCP Configured");
    }

    // *************************************************************************
    // ConnectionProvider
    // *************************************************************************

    @Override
    public Connection getConnection() throws SQLException {
        Connection conn = null;
        if (hds != null) {
            conn = hds.getConnection();
        }

        return conn;
    }

    @Override
    public void closeConnection(@NotNull Connection conn) throws SQLException {
        conn.close();
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
            return (T) hds;
        } else {
            throw new UnknownUnwrapTypeException(unwrapType);
        }
    }

    // *************************************************************************
    // Stoppable
    // *************************************************************************

    @Override
    public void stop() {
        hds.close();
    }
}
