package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.SQLExceptionOverride;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import com.zaxxer.hikari.util.DriverDataSource;
import com.zaxxer.hikari.util.PropertyElf;
import com.zaxxer.hikari.util.UtilityElf;
import com.zaxxer.hikari.util.UtilityElf.DefaultThreadFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static com.zaxxer.hikari.pool.ProxyConnection.*;
import static com.zaxxer.hikari.util.ClockSource.*;
import static com.zaxxer.hikari.util.UtilityElf.createInstance;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Getter
abstract class PoolBase {

    private final Logger logger = LoggerFactory.getLogger(PoolBase.class);

    public final HikariConfig config;
    IMetricsTrackerDelegate metricsTracker;

    protected final String poolName;

    volatile String catalog;
    final AtomicReference<Exception> lastConnectionFailure;

    long connectionTimeout;
    long validationTimeout;

    SQLExceptionOverride exceptionOverride;

    private static final String[] RESET_STATES = {"readOnly", "autoCommit",
            "isolation", "catalog", "netTimeout", "schema"};

    private static final int UNINITIALIZED = -1;
    private static final int TRUE = 1;
    private static final int FALSE = 0;

    private int networkTimeout;
    private int isNetworkTimeoutSupported;
    private int isQueryTimeoutSupported;
    private int defaultTransactionIsolation;
    private int transactionIsolation;
    private Executor netTimeoutExecutor;
    private DataSource unwrappedDataSource;

    private final String schema;
    private final boolean isReadOnly;
    private final boolean isAutoCommit;
    private final boolean isUseJdbc4Validation;
    private final boolean isIsolateInternalQueries;

    private volatile boolean isValidChecked;

    PoolBase(@NotNull HikariConfig config) {
        this.config = config;

        networkTimeout = UNINITIALIZED;
        catalog = config.getCatalog();
        schema = config.getSchema();
        isReadOnly = config.isReadOnly();
        isAutoCommit = config.isAutoCommit();

        exceptionOverride = UtilityElf.createInstance(
                config.getExceptionOverrideClassName(), SQLExceptionOverride.class
        );

        transactionIsolation = UtilityElf.getTransactionIsolation(config.getTransactionIsolation());

        isQueryTimeoutSupported = UNINITIALIZED;
        isNetworkTimeoutSupported = UNINITIALIZED;
        isUseJdbc4Validation = config.getConnectionTestQuery() == null;
        isIsolateInternalQueries = config.isIsolateInternalQueries();

        poolName = config.getPoolName();
        connectionTimeout = config.getConnectionTimeout();
        validationTimeout = config.getValidationTimeout();
        lastConnectionFailure = new AtomicReference<>();

        initializeDataSource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return poolName;
    }

    abstract void recycle(PoolEntry poolEntry);

    // ***********************************************************************
    //                           JDBC methods
    // ***********************************************************************

    void quietlyCloseConnection(Connection connection, String closureReason) {
        if (connection != null) {
            try {
                logger.debug("{} - Closing connection {}: {}", poolName, connection, closureReason);

                try {
                    setNetworkTimeout(connection, SECONDS.toMillis(15));
                } catch (SQLException ignored) {
                } finally {
                    connection.close(); // continue with the close even if setNetworkTimeout() throws
                }
            } catch (Exception ex) {
                logger.debug("{} - Closing connection {} failed", poolName, connection, ex);
            }
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean isConnectionAlive(Connection connection) {
        try {
            try {
                setNetworkTimeout(connection, validationTimeout);

                int validationSeconds = (int) Math.max(1000L, validationTimeout) / 1000;

                if (isUseJdbc4Validation) {
                    return connection.isValid(validationSeconds);
                }

                try (Statement statement = connection.createStatement()) {
                    if (isNetworkTimeoutSupported != TRUE) {
                        setQueryTimeout(statement, validationSeconds);
                    }

                    statement.execute(config.getConnectionTestQuery());
                }
            } finally {
                setNetworkTimeout(connection, networkTimeout);

                if (isIsolateInternalQueries && !isAutoCommit) {
                    connection.rollback();
                }
            }

            return true;
        } catch (Exception ex) {
            lastConnectionFailure.set(ex);
            logger.warn("{} - Failed to validate connection {} ({})."
                    + " Possibly consider using a shorter maxLifetime value.", poolName, connection, ex.getMessage());
            return false;
        }
    }

    Exception getLastConnectionFailure() {
        return lastConnectionFailure.get();
    }

    // ***********************************************************************
    //                         PoolEntry methods
    // ***********************************************************************

    PoolEntry newPoolEntry() throws Exception {
        return new PoolEntry(newConnection(), this, isReadOnly, isAutoCommit);
    }

    void resetConnectionState(Connection connection, ProxyConnection proxyConnection,
                              int dirtyBits) throws SQLException {
        int resetBits = 0;

        if ((dirtyBits & DIRTY_BIT_READONLY) != 0
                && proxyConnection.getReadOnlyState() != isReadOnly) {
            connection.setReadOnly(isReadOnly);
            resetBits |= DIRTY_BIT_READONLY;
        }

        if ((dirtyBits & DIRTY_BIT_AUTOCOMMIT) != 0
                && proxyConnection.getAutoCommitState() != isAutoCommit) {
            connection.setAutoCommit(isAutoCommit);
            resetBits |= DIRTY_BIT_AUTOCOMMIT;
        }

        if ((dirtyBits & DIRTY_BIT_ISOLATION) != 0
                && proxyConnection.getTransactionIsolationState() != transactionIsolation) {
            connection.setTransactionIsolation(transactionIsolation);
            resetBits |= DIRTY_BIT_ISOLATION;
        }

        if ((dirtyBits & DIRTY_BIT_CATALOG) != 0
                && catalog != null && !catalog.equals(proxyConnection.getCatalogState())) {
            connection.setCatalog(catalog);
            resetBits |= DIRTY_BIT_CATALOG;
        }

        if ((dirtyBits & DIRTY_BIT_NETTIMEOUT) != 0
                && proxyConnection.getNetworkTimeoutState() != networkTimeout) {
            setNetworkTimeout(connection, networkTimeout);
            resetBits |= DIRTY_BIT_NETTIMEOUT;
        }

        if ((dirtyBits & DIRTY_BIT_SCHEMA) != 0
                && schema != null && !schema.equals(proxyConnection.getSchemaState())) {
            connection.setSchema(schema);
            resetBits |= DIRTY_BIT_SCHEMA;
        }

        if (resetBits != 0 && logger.isDebugEnabled()) {
            logger.debug("{} - Reset ({}) on connection {}", poolName, stringFromResetBits(resetBits), connection);
        }
    }

    void shutdownNetworkTimeoutExecutor() {
        if (netTimeoutExecutor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) netTimeoutExecutor).shutdownNow();
        }
    }

    long getLoginTimeout() {
        try {
            return (unwrappedDataSource != null) ? unwrappedDataSource.getLoginTimeout() : SECONDS.toSeconds(5);
        } catch (SQLException ex) {
            return SECONDS.toSeconds(5);
        }
    }

    // ***********************************************************************
    //                       JMX methods
    // ***********************************************************************

    /**
     * Register MBeans for HikariConfig and HikariPool.
     *
     * @param hikariPool a HikariPool instance
     */
    void handleMBeans(HikariPool hikariPool, boolean register) {
        if (!config.isRegisterMbeans()) {
            return;
        }

        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName beanConfigName;
            ObjectName beanPoolName;

            if ("true".equals(System.getProperty("hikaricp.jmx.register2.0"))) {
                beanConfigName = new ObjectName("com.zaxxer.hikari:type=PoolConfig,name=" + poolName);
                beanPoolName = new ObjectName("com.zaxxer.hikari:type=Pool,name=" + poolName);
            } else {
                beanConfigName = new ObjectName("com.zaxxer.hikari:type=PoolConfig (" + poolName + ")");
                beanPoolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + poolName + ")");
            }

            if (register) {
                if (!mBeanServer.isRegistered(beanConfigName)) {
                    mBeanServer.registerMBean(config, beanConfigName);
                    mBeanServer.registerMBean(hikariPool, beanPoolName);
                } else {
                    logger.error("{} - JMX name ({}) is already registered.", poolName, poolName);
                }
            } else if (mBeanServer.isRegistered(beanConfigName)) {
                mBeanServer.unregisterMBean(beanConfigName);
                mBeanServer.unregisterMBean(beanPoolName);
            }
        } catch (Exception ex) {
            logger.warn("{} - Failed to {} management beans.", poolName, (register ? "register" : "unregister"), ex);
        }
    }

    // ***********************************************************************
    //                          Private methods
    // ***********************************************************************

    /**
     * Create/initialize the underlying DataSource.
     */
    private void initializeDataSource() {
        String jdbcUrl = config.getJdbcUrl();
        String username = config.getUsername();
        String password = config.getPassword();
        String dsClassName = config.getDataSourceClassName();
        String driverClassName = config.getDriverClassName();
        String dataSourceJNDI = config.getDataSourceJNDI();
        Properties dataSourceProperties = config.getDataSourceProperties();
        DataSource ds = config.getDataSource();

        if (dsClassName != null && ds == null) {
            ds = createInstance(dsClassName, DataSource.class);
            PropertyElf.setTargetFromProperties(ds, dataSourceProperties);

        } else if (jdbcUrl != null && ds == null) {
            ds = new DriverDataSource(jdbcUrl, driverClassName, dataSourceProperties, username, password);

        } else if (dataSourceJNDI != null && ds == null) {
            try {
                InitialContext ic = new InitialContext();
                ds = (DataSource) ic.lookup(dataSourceJNDI);
            } catch (NamingException ex) {
                throw new PoolInitializationException(ex);
            }
        }

        if (ds != null) {
            setLoginTimeout(ds);
            createNetworkTimeoutExecutor(ds, dsClassName, jdbcUrl);
        }

        unwrappedDataSource = ds;
    }

    /**
     * Obtain connection from data source.
     *
     * @return a Connection
     */
    private @NotNull Connection newConnection() throws Exception {
        long start = currentTime();
        Connection connection = null;

        try {
            String username = config.getUsername();
            String password = config.getPassword();

            connection = (username == null)
                    ? unwrappedDataSource.getConnection()
                    : unwrappedDataSource.getConnection(username, password);

            if (connection == null) {
                throw new SQLTransientConnectionException("DataSource returned null unexpectedly");
            }

            setupConnection(connection);
            lastConnectionFailure.set(null);
            return connection;

        } catch (Exception ex) {
            if (connection != null) {
                quietlyCloseConnection(connection, "(Failed to create/setup connection)");
            } else if (getLastConnectionFailure() == null) {
                logger.debug("{} - Failed to create/setup connection: {}", poolName, ex.getMessage());
            }

            lastConnectionFailure.set(ex);
            throw ex;

        } finally {
            // tracker will be null during failFast check
            if (metricsTracker != null) {
                metricsTracker.recordConnectionCreated(elapsedMillis(start));
            }
        }
    }

    /**
     * Set up a connection initial state.
     *
     * @param connection a Connection
     * @throws ConnectionSetupException thrown if any exception is encountered
     */
    private void setupConnection(Connection connection) throws ConnectionSetupException {
        try {
            if (networkTimeout == UNINITIALIZED) {
                networkTimeout = getAndSetNetworkTimeout(connection, validationTimeout);
            } else {
                setNetworkTimeout(connection, validationTimeout);
            }

            if (connection.isReadOnly() != isReadOnly) {
                connection.setReadOnly(isReadOnly);
            }

            if (connection.getAutoCommit() != isAutoCommit) {
                connection.setAutoCommit(isAutoCommit);
            }

            checkDriverSupport(connection);

            if (transactionIsolation != defaultTransactionIsolation) {
                connection.setTransactionIsolation(transactionIsolation);
            }

            if (catalog != null) {
                connection.setCatalog(catalog);
            }

            if (schema != null) {
                connection.setSchema(schema);
            }

            executeSql(connection, config.getConnectionInitSql(), true);
            setNetworkTimeout(connection, networkTimeout);
        } catch (SQLException ex) {
            throw new ConnectionSetupException(ex);
        }
    }

    /**
     * Execute isValid() or connection test query.
     *
     * @param connection a Connection to check
     */
    private void checkDriverSupport(Connection connection) throws SQLException {
        if (!isValidChecked) {
            checkValidationSupport(connection);
            checkDefaultIsolation(connection);

            isValidChecked = true;
        }
    }

    /**
     * Check whether Connection.isValid() is supported, or that the user has test query configured.
     *
     * @param connection a Connection to check
     * @throws SQLException rethrown from the driver
     */
    private void checkValidationSupport(Connection connection) throws SQLException {
        try {
            if (isUseJdbc4Validation) {
                connection.isValid(1);
            } else {
                executeSql(connection, config.getConnectionTestQuery(), false);
            }
        } catch (Exception | AbstractMethodError ex) {
            logger.error("{} - Failed to execute{} connection test query ({}).", poolName,
                    (isUseJdbc4Validation ? " isValid() for connection, configure" : ""), ex.getMessage());
            throw ex;
        }
    }

    /**
     * Check the default transaction isolation of the Connection.
     *
     * @param connection a Connection to check
     * @throws SQLException rethrown from the driver
     */
    private void checkDefaultIsolation(@NotNull Connection connection) throws SQLException {
        try {
            defaultTransactionIsolation = connection.getTransactionIsolation();

            if (transactionIsolation == -1) {
                transactionIsolation = defaultTransactionIsolation;
            }
        } catch (SQLException ex) {
            logger.warn("{} - Default transaction isolation level detection failed ({}).", poolName, ex.getMessage());

            if (ex.getSQLState() != null && !ex.getSQLState().startsWith("08")) {
                throw ex;
            }
        }
    }

    /**
     * Set the query timeout, if it is supported by the driver.
     *
     * @param statement  a statement to set the query timeout on
     * @param timeoutSec the number of seconds before timeout
     */
    private void setQueryTimeout(Statement statement, int timeoutSec) {
        if (isQueryTimeoutSupported != FALSE) {
            try {
                statement.setQueryTimeout(timeoutSec);
                isQueryTimeoutSupported = TRUE;
            } catch (Exception ex) {
                if (isQueryTimeoutSupported == UNINITIALIZED) {
                    isQueryTimeoutSupported = FALSE;
                    logger.info("{} - Failed to set query timeout for statement. ({})", poolName, ex.getMessage());
                }
            }
        }
    }

    /**
     * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
     * driver supports it.  Return the pre-existing value of the network timeout.
     *
     * @param connection the connection to set the network timeout on
     * @param timeoutMs  the number of milliseconds before timeout
     * @return the pre-existing network timeout value
     */
    private int getAndSetNetworkTimeout(Connection connection, long timeoutMs) {
        if (isNetworkTimeoutSupported != FALSE) {
            try {
                int originalTimeout = connection.getNetworkTimeout();
                connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
                isNetworkTimeoutSupported = TRUE;
                return originalTimeout;
            } catch (Exception | AbstractMethodError ex) {
                if (isNetworkTimeoutSupported == UNINITIALIZED) {
                    isNetworkTimeoutSupported = FALSE;

                    logger.info("{} - Driver does not support get/set network timeout for connections. ({})",
                            poolName, ex.getMessage());

                    if (validationTimeout < SECONDS.toMillis(1)) {
                        logger.warn("{} - A validationTimeout of less than 1 second cannot be honored on drivers"
                                + " without setNetworkTimeout() support.", poolName);
                    } else if (validationTimeout % SECONDS.toMillis(1) != 0) {
                        logger.warn("{} - A validationTimeout with fractional second granularity cannot be honored"
                                + " on drivers without setNetworkTimeout() support.", poolName);
                    }
                }
            }
        }
        return 0;
    }

    /**
     * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
     * driver supports it.
     *
     * @param connection the connection to set the network timeout on
     * @param timeoutMs  the number of milliseconds before timeout
     * @throws SQLException throw if the connection.setNetworkTimeout() call throws
     */
    private void setNetworkTimeout(Connection connection, long timeoutMs) throws SQLException {
        if (isNetworkTimeoutSupported == TRUE) {
            connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
        }
    }

    /**
     * Execute the user-specified init SQL.
     *
     * @param connection the connection to initialize
     * @param sql        the SQL to execute
     * @param isCommit   whether to commit the SQL after execution or not
     * @throws SQLException throws if the init SQL execution fails
     */
    private void executeSql(Connection connection, String sql, boolean isCommit) throws SQLException {
        if (sql != null) {
            try (Statement statement = connection.createStatement()) {
                // connection was created a few milliseconds before,
                // so set query timeout is omitted (we assume it will succeed)
                statement.execute(sql);
            }

            if (isIsolateInternalQueries && !isAutoCommit) {
                if (isCommit) {
                    connection.commit();
                } else {
                    connection.rollback();
                }
            }
        }
    }

    private void createNetworkTimeoutExecutor(DataSource dataSource, String dsClassName, String jdbcUrl) {
        // Temporary hack for MySQL issue: http://bugs.mysql.com/bug.php?id=75615
        if ((dsClassName != null && dsClassName.contains("Mysql"))
                || (jdbcUrl != null && jdbcUrl.contains("mysql"))
                || (dataSource != null && dataSource.getClass().getName().contains("Mysql"))) {
            netTimeoutExecutor = new SynchronousExecutor();
        } else {
            ThreadFactory threadFactory = config.getThreadFactory();
            threadFactory = threadFactory != null ? threadFactory
                    : new DefaultThreadFactory(poolName + " network timeout executor", true);

            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);
            executor.setKeepAliveTime(15, SECONDS);
            executor.allowCoreThreadTimeOut(true);
            netTimeoutExecutor = executor;
        }
    }

    /**
     * Set the loginTimeout on the specified DataSource.
     *
     * @param dataSource the DataSource
     */
    private void setLoginTimeout(DataSource dataSource) {
        if (connectionTimeout != Integer.MAX_VALUE) {
            try {
                dataSource.setLoginTimeout(Math.max(1, (int) MILLISECONDS.toSeconds(500L + connectionTimeout)));
            } catch (Exception ex) {
                logger.info("{} - Failed to set login timeout for data source. ({})", poolName, ex.getMessage());
            }
        }
    }

    /**
     * This will create a string for debug logging. Given a set of "reset bits", this
     * method will return a concatenated string, for example:
     * <p>
     * Input : 0b00110
     * Output: "autoCommit, isolation"
     *
     * @param bits a set of "reset bits"
     * @return a string of which states were reset
     */
    private @NotNull String stringFromResetBits(int bits) {
        StringBuilder sb = new StringBuilder();

        for (int ndx = 0; ndx < RESET_STATES.length; ndx++) {
            if ((bits & (0b1 << ndx)) != 0) {
                sb.append(RESET_STATES[ndx]).append(", ");
            }
        }

        sb.setLength(sb.length() - 2); // trim trailing comma
        return sb.toString();
    }

    // ***********************************************************************
    //                      Private Static Classes
    // ***********************************************************************

    static class ConnectionSetupException extends Exception {

        private static final long serialVersionUID = 929872118275916521L;

        ConnectionSetupException(Throwable ex) {
            super(ex);
        }
    }

    /**
     * Special executor used only to work around a MySQL issue that has not been addressed.
     * MySQL issue: <a href="http://bugs.mysql.com/bug.php?id=75615">MySQL Bug Report</a>
     */
    private static class SynchronousExecutor implements Executor {

        /**
         * {@inheritDoc}
         */
        @Override
        public void execute(@NotNull Runnable command) {
            try {
                command.run();
            } catch (Exception ex) {
                LoggerFactory.getLogger(PoolBase.class).debug("Failed to execute: {}", command, ex);
            }
        }
    }

    interface IMetricsTrackerDelegate extends AutoCloseable {

        default void recordConnectionUsage(PoolEntry poolEntry) {
        }

        default void recordConnectionCreated(long connectionCreatedMillis) {
        }

        default void recordBorrowTimeoutStats(long startTime) {
        }

        default void recordBorrowStats(PoolEntry poolEntry, long startTime) {
        }

        default void recordConnectionTimeout() {
        }

        @Override
        default void close() {
        }
    }

    /**
     * A class that delegates to a MetricsTracker implementation.  The use of a delegate
     * allows us to use the NopMetricsTrackerDelegate when metrics are disabled, which in
     * turn allows the JIT to completely optimize away to callsites to record metrics.
     */
    @AllArgsConstructor
    static class MetricsTrackerDelegate implements IMetricsTrackerDelegate {

        final IMetricsTracker tracker;

        @Override
        public void recordConnectionUsage(@NotNull PoolEntry poolEntry) {
            tracker.recordConnectionUsageMillis(poolEntry.getMillisSinceBorrowed());
        }

        @Override
        public void recordConnectionCreated(long connectionCreatedMillis) {
            tracker.recordConnectionCreatedMillis(connectionCreatedMillis);
        }

        @Override
        public void recordBorrowTimeoutStats(long startTime) {
            tracker.recordConnectionAcquiredNanos(elapsedNanos(startTime));
        }

        @Override
        public void recordBorrowStats(@NotNull PoolEntry poolEntry, long startTime) {
            long now = currentTime();
            poolEntry.lastBorrowed = now;
            tracker.recordConnectionAcquiredNanos(elapsedNanos(startTime, now));
        }

        @Override
        public void recordConnectionTimeout() {
            tracker.recordConnectionTimeout();
        }

        @Override
        public void close() {
            tracker.close();
        }
    }

    /**
     * A no-op implementation of the IMetricsTrackerDelegate that is used when metrics capture is
     * disabled.
     */
    static final class NopMetricsTrackerDelegate implements IMetricsTrackerDelegate {

    }
}
