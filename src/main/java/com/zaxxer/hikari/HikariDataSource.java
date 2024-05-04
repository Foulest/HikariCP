package com.zaxxer.hikari;

import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.pool.HikariPool;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.zaxxer.hikari.pool.HikariPool.POOL_NORMAL;

/**
 * The HikariCP pooled DataSource.
 *
 * @author Brett Wooldridge
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
@SuppressWarnings("unused")
public class HikariDataSource extends HikariConfig implements DataSource, Closeable {

    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final HikariPool fastPathPool;

    @ToString.Include
    private volatile HikariPool pool;

    /**
     * Default constructor.  Setters are used to configure the pool.  Using
     * this constructor vs. {@link #HikariDataSource(HikariConfig)} will
     * result in {@link #getConnection()} performance that is slightly lower
     * due to lazy initialization checks.
     * <p>
     * The first call to {@link #getConnection()} starts the pool.  Once the pool
     * is started, the configuration is "sealed" and no further configuration
     * changes are possible -- except via {@link HikariConfigMXBean} methods.
     */
    public HikariDataSource() {
        fastPathPool = null;
    }

    /**
     * Construct a HikariDataSource with the specified configuration.  The
     * {@link HikariConfig} is copied and the pool is started by invoking this
     * constructor.
     * <p>
     * The {@link HikariConfig} can be modified without affecting the HikariDataSource
     * and used to initialize another HikariDataSource instance.
     *
     * @param configuration a HikariConfig instance
     */
    public HikariDataSource(@NotNull HikariConfig configuration) {
        configuration.validate();
        configuration.copyStateTo(this);

        log.info("{} - Starting HikariDataSource...", configuration.getPoolName());
        pool = fastPathPool = new HikariPool(this);
        log.info("{} - HikariDataSource start completed.", configuration.getPoolName());

        seal();
    }

    // ***********************************************************************
    //                          DataSource methods
    // ***********************************************************************

    @Override
    public Connection getConnection() throws SQLException {
        if (isClosed()) {
            throw new SQLException("HikariDataSource " + this + " has been closed.");
        }

        if (fastPathPool != null) {
            return fastPathPool.getConnection();
        }

        // See http://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java
        HikariPool hikariPool = pool;

        if (hikariPool == null) {
            synchronized (this) {
                hikariPool = pool;

                if (hikariPool == null) {
                    validate();
                    log.info("{} - Starting Connection...", getPoolName());

                    try {
                        pool = hikariPool = new HikariPool(this);
                        seal();
                    } catch (PoolInitializationException pie) {
                        if (pie.getCause() instanceof SQLException) {
                            throw (SQLException) pie.getCause();
                        } else {
                            throw pie;
                        }
                    }

                    log.info("{} - Connection start completed.", getPoolName());
                }
            }
        }
        return hikariPool.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        HikariPool hikariPool = pool;
        return (hikariPool != null ? hikariPool.getUnwrappedDataSource().getLogWriter() : null);
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        HikariPool hikariPool = pool;

        if (hikariPool != null) {
            hikariPool.getUnwrappedDataSource().setLogWriter(out);
        }
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        HikariPool hikariPool = pool;

        if (hikariPool != null) {
            hikariPool.getUnwrappedDataSource().setLoginTimeout(seconds);
        }
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        HikariPool hikariPool = pool;
        return (hikariPool != null ? hikariPool.getUnwrappedDataSource().getLoginTimeout() : 0);
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(@NotNull Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }

        HikariPool hikariPool = pool;

        if (hikariPool != null) {
            DataSource unwrappedDataSource = hikariPool.getUnwrappedDataSource();

            if (iface.isInstance(unwrappedDataSource)) {
                return (T) unwrappedDataSource;
            }

            if (unwrappedDataSource != null) {
                return unwrappedDataSource.unwrap(iface);
            }
        }
        throw new SQLException("Wrapped DataSource is not an instance of " + iface);
    }

    @Override
    public boolean isWrapperFor(@NotNull Class<?> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return true;
        }

        HikariPool hikariPool = pool;

        if (hikariPool != null) {
            DataSource unwrappedDataSource = hikariPool.getUnwrappedDataSource();

            if (iface.isInstance(unwrappedDataSource)) {
                return true;
            }

            if (unwrappedDataSource != null) {
                return unwrappedDataSource.isWrapperFor(iface);
            }
        }
        return false;
    }

    // ***********************************************************************
    //                        HikariConfigMXBean methods
    // ***********************************************************************

    @Override
    public void setMetricRegistry(Object metricRegistry) {
        boolean isAlreadySet = getMetricRegistry() != null;
        super.setMetricRegistry(metricRegistry);

        HikariPool hikariPool = pool;

        if (hikariPool != null) {
            if (isAlreadySet) {
                throw new IllegalStateException("MetricRegistry can only be set one time");
            } else {
                hikariPool.setMetricRegistry(getMetricRegistry());
            }
        }
    }

    @Override
    public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
        boolean isAlreadySet = getMetricsTrackerFactory() != null;
        super.setMetricsTrackerFactory(metricsTrackerFactory);

        HikariPool hikariPool = pool;

        if (hikariPool != null) {
            if (isAlreadySet) {
                throw new IllegalStateException("MetricsTrackerFactory can only be set one time");
            } else {
                hikariPool.setMetricsTrackerFactory(getMetricsTrackerFactory());
            }
        }
    }

    @Override
    public void setHealthCheckRegistry(Object healthCheckRegistry) {
        boolean isAlreadySet = getHealthCheckRegistry() != null;
        super.setHealthCheckRegistry(healthCheckRegistry);

        HikariPool hikariPool = pool;

        if (hikariPool != null) {
            if (isAlreadySet) {
                throw new IllegalStateException("HealthCheckRegistry can only be set one time");
            } else {
                hikariPool.setHealthCheckRegistry(getHealthCheckRegistry());
            }
        }
    }

    // ***********************************************************************
    //                        HikariCP-specific methods
    // ***********************************************************************

    /**
     * Returns {@code true} if the pool as been started and is not suspended or shutdown.
     *
     * @return {@code true} if the pool as been started and is not suspended or shutdown.
     */
    public boolean isRunning() {
        return pool != null && pool.poolState == POOL_NORMAL;
    }

    /**
     * Get the {@code HikariPoolMXBean} for this HikariDataSource instance.  If this method is called on
     * a {@code HikariDataSource} that has been constructed without a {@code HikariConfig} instance,
     * and before an initial call to {@code #getConnection()}, the return value will be {@code null}.
     *
     * @return the {@code HikariPoolMXBean} instance, or {@code null}.
     */
    public HikariPoolMXBean getHikariPoolMXBean() {
        return pool;
    }

    /**
     * Get the {@code HikariConfigMXBean} for this HikariDataSource instance.
     *
     * @return the {@code HikariConfigMXBean} instance.
     */
    public HikariConfigMXBean getHikariConfigMXBean() {
        return this;
    }

    /**
     * Evict a connection from the pool.  If the connection has already been closed (returned to the pool)
     * this may result in a "soft" eviction; the connection will be evicted sometime in the future if it is
     * currently in use.  If the connection has not been closed, the eviction is immediate.
     *
     * @param connection the connection to evict from the pool
     */
    public void evictConnection(Connection connection) {
        HikariPool hikariPool;

        if (!isClosed() && (hikariPool = pool) != null
                && connection.getClass().getName().startsWith("com.zaxxer.hikari")) {
            hikariPool.evictConnection(connection);
        }
    }

    /**
     * Shutdown the DataSource and its associated pool.
     */
    @Override
    public void close() {
        if (isShutdown.getAndSet(true)) {
            return;
        }

        HikariPool hikariPool = pool;

        if (hikariPool != null) {
            try {
                log.info("{} - Shutdown initiated...", getPoolName());
                hikariPool.shutdown();
                log.info("{} - Shutdown completed.", getPoolName());
            } catch (InterruptedException ex) {
                log.warn("{} - Interrupted during closing", getPoolName(), ex);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Determine whether the HikariDataSource has been closed.
     *
     * @return true if the HikariDataSource has been closed, false otherwise
     */
    public boolean isClosed() {
        return isShutdown.get();
    }
}
