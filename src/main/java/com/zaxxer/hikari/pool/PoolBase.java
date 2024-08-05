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
package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.SQLExceptionOverride;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.util.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import javax.management.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Getter
@ToString(onlyExplicitlyIncluded = true)
abstract class PoolBase {

    public final HikariConfig config;
    IMetricsTrackerDelegate metricsTracker;

    @ToString.Include
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
    private volatile int isNetworkTimeoutSupported;
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

        exceptionOverride = config.getExceptionOverride();

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

    @SuppressWarnings("unused")
    abstract void recycle(PoolEntry poolEntry);

    // ***********************************************************************
    //                           JDBC methods
    // ***********************************************************************

    void quietlyCloseConnection(Connection connection, String closureReason) {
        if (connection != null) {
            setNetworkTimeoutAndClose(connection, closureReason);
        }
    }

    private void setNetworkTimeoutSafely(Connection connection) {
        try {
            setNetworkTimeout(connection, TimeUnit.SECONDS.toMillis(15));
        } catch (SQLException ex) {
            log.debug("{} - Setting network timeout failed for connection {}", poolName, connection, ex);
        }
    }

    private void setNetworkTimeoutAndClose(Connection connection, String closureReason) {
        log.debug("{} - Closing connection {}: {}", poolName, connection, closureReason);
        setNetworkTimeoutSafely(connection);
        closeConnectionSafely(connection);
    }

    private void closeConnectionSafely(@NotNull Connection connection) {
        try {
            connection.close();
        } catch (SQLException ex) {
            log.debug("{} - Closing connection {} failed", poolName, connection, ex);
        }
    }

    boolean isConnectionDead(Connection connection) {
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
        } catch (SQLException ex) {
            lastConnectionFailure.set(ex);
            log.warn("{} - Failed to validate connection {} ({})."
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

    PoolEntry newPoolEntry() throws SQLException, ConnectionSetupException {
        return new PoolEntry(newConnection(), this, isReadOnly, isAutoCommit);
    }

    void resetConnectionState(Connection connection, ProxyConnection proxyConnection,
                              int dirtyBits) throws SQLException {
        int resetBits = 0;

        if ((dirtyBits & ProxyConnection.DIRTY_BIT_READONLY) != 0
                && proxyConnection.getReadOnlyState() != isReadOnly) {
            connection.setReadOnly(isReadOnly);
            resetBits |= ProxyConnection.DIRTY_BIT_READONLY;
        }

        if ((dirtyBits & ProxyConnection.DIRTY_BIT_AUTOCOMMIT) != 0
                && proxyConnection.getAutoCommitState() != isAutoCommit) {
            connection.setAutoCommit(isAutoCommit);
            resetBits |= ProxyConnection.DIRTY_BIT_AUTOCOMMIT;
        }

        if ((dirtyBits & ProxyConnection.DIRTY_BIT_ISOLATION) != 0
                && proxyConnection.getTransactionIsolationState() != transactionIsolation) {
            connection.setTransactionIsolation(transactionIsolation);
            resetBits |= ProxyConnection.DIRTY_BIT_ISOLATION;
        }

        if ((dirtyBits & ProxyConnection.DIRTY_BIT_CATALOG) != 0
                && catalog != null && !catalog.equals(proxyConnection.getCatalogState())) {
            connection.setCatalog(catalog);
            resetBits |= ProxyConnection.DIRTY_BIT_CATALOG;
        }

        if ((dirtyBits & ProxyConnection.DIRTY_BIT_NETTIMEOUT) != 0
                && proxyConnection.getNetworkTimeoutState() != networkTimeout) {
            setNetworkTimeout(connection, networkTimeout);
            resetBits |= ProxyConnection.DIRTY_BIT_NETTIMEOUT;
        }

        if ((dirtyBits & ProxyConnection.DIRTY_BIT_SCHEMA) != 0
                && schema != null && !schema.equals(proxyConnection.getSchemaState())) {
            connection.setSchema(schema);
            resetBits |= ProxyConnection.DIRTY_BIT_SCHEMA;
        }

        if (resetBits != 0 && log.isDebugEnabled()) {
            log.debug("{} - Reset ({}) on connection {}", poolName, stringFromResetBits(resetBits), connection);
        }
    }

    void shutdownNetworkTimeoutExecutor() {
        isNetworkTimeoutSupported = UNINITIALIZED; // #2147

        if (netTimeoutExecutor instanceof ThreadPoolExecutor) {
            ((ExecutorService) netTimeoutExecutor).shutdownNow();
        }
    }

    long getLoginTimeout() {
        try {
            return (unwrappedDataSource != null) ? unwrappedDataSource.getLoginTimeout() : TimeUnit.SECONDS.toSeconds(5);
        } catch (SQLException ex) {
            return TimeUnit.SECONDS.toSeconds(5);
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
                if (mBeanServer.isRegistered(beanConfigName)) {
                    log.error("{} - JMX name ({}) is already registered.", poolName, poolName);
                } else {
                    mBeanServer.registerMBean(config, beanConfigName);
                    mBeanServer.registerMBean(hikariPool, beanPoolName);
                }
            } else if (mBeanServer.isRegistered(beanConfigName)) {
                mBeanServer.unregisterMBean(beanConfigName);
                mBeanServer.unregisterMBean(beanPoolName);
            }
        } catch (InstanceAlreadyExistsException | InstanceNotFoundException | MBeanRegistrationException
                 | MalformedObjectNameException | NotCompliantMBeanException ex) {
            log.warn("{} - Failed to {} management beans.", poolName, (register ? "register" : "unregister"), ex);
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
        Credentials credentials = config.getCredentials();
        String dsClassName = config.getDataSourceClassName();
        String driverClassName = config.getDriverClassName();
        String dataSourceJNDI = config.getDataSourceJNDI();
        Map<Object, Object> dataSourceProperties = config.getDataSourceProperties();
        DataSource ds = config.getDataSource();

        if (dsClassName != null && ds == null) {
            ds = UtilityElf.createInstance(dsClassName, DataSource.class);
            PropertyElf.setTargetFromProperties(ds, dataSourceProperties);

        } else if (jdbcUrl != null && ds == null) {
            ds = new DriverDataSource(jdbcUrl, driverClassName, dataSourceProperties,
                    credentials.getUsername(), credentials.getPassword());

        } else if (dataSourceJNDI != null && ds == null) {
            InitialContext ic = null;
            try {
                ic = new InitialContext();
                ds = (DataSource) ic.lookup(dataSourceJNDI);
            } catch (NamingException ex) {
                throw new HikariPool.PoolInitializationException(ex);
            } finally {
                if (ic != null) {
                    try {
                        ic.close();
                    } catch (NamingException ex) {
                        ex.printStackTrace();
                    }
                }
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
    private @NotNull Connection newConnection() throws SQLException, ConnectionSetupException {
        long start = ClockSource.currentTime();
        Connection connection = null;

        try {
            connection = createConnection();
            setupConnection(connection);
            lastConnectionFailure.set(null);
            return connection;
        } catch (SQLTransientConnectionException | ConnectionSetupException ex) {
            handleConnectionException(connection, ex);
            throw ex;
        } finally {
            recordMetrics(start);
        }
    }

    /**
     * Create a new connection using the data source and credentials.
     *
     * @return a new Connection
     * @throws SQLException if a database access error occurs
     */
    private @NotNull Connection createConnection() throws SQLException {
        Credentials credentials = config.getCredentials();
        String username = credentials.getUsername();
        String password = credentials.getPassword();

        Connection connection = (username == null)
                ? unwrappedDataSource.getConnection()
                : unwrappedDataSource.getConnection(username, password);

        if (connection == null) {
            throw new SQLTransientConnectionException("DataSource returned null unexpectedly");
        }
        return connection;
    }

    /**
     * Handle exceptions that occur during connection setup.
     *
     * @param connection the connection that was attempted to be created
     * @param ex the exception that was thrown
     */
    private void handleConnectionException(Connection connection, Exception ex) {
        if (connection != null) {
            quietlyCloseConnection(connection, "(Failed to create/setup connection)");
        } else if (getLastConnectionFailure() == null) {
            log.debug("{} - Failed to create/setup connection: {}", poolName, ex.getMessage());
        }

        lastConnectionFailure.set(ex);
    }

    /**
     * Record metrics for connection creation time.
     *
     * @param start the start time of the connection creation
     */
    private void recordMetrics(long start) {
        if (metricsTracker != null) {
            metricsTracker.recordConnectionCreated(ClockSource.elapsedMillis(start));
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
        } catch (SQLException | AbstractMethodError ex) {
            log.error("{} - Failed to execute{} connection test query ({}).", poolName,
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
            log.warn("{} - Default transaction isolation level detection failed ({}).", poolName, ex.getMessage());

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
            } catch (SQLException ex) {
                if (isQueryTimeoutSupported == UNINITIALIZED) {
                    isQueryTimeoutSupported = FALSE;
                    log.info("{} - Failed to set query timeout for statement. ({})", poolName, ex.getMessage());
                }
            }
        }
    }

    /**
     * Set the network timeout, if {@code isUseNetworkTimeout} is {@code true} and the
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
            } catch (SQLException | AbstractMethodError ex) {
                if (isNetworkTimeoutSupported == UNINITIALIZED) {
                    isNetworkTimeoutSupported = FALSE;

                    log.info("{} - Driver does not support get/set network timeout for connections. ({})",
                            poolName, ex.getMessage());

                    if (validationTimeout < TimeUnit.SECONDS.toMillis(1)) {
                        log.warn("{} - A validationTimeout of less than 1 second cannot be honored on drivers"
                                + " without setNetworkTimeout() support.", poolName);
                    } else if (validationTimeout % TimeUnit.SECONDS.toMillis(1) != 0) {
                        log.warn("{} - A validationTimeout with fractional second granularity cannot be honored"
                                + " on drivers without setNetworkTimeout() support.", poolName);
                    }
                }
            }
        }
        return 0;
    }

    /**
     * Set the network timeout, if {@code isUseNetworkTimeout} is {@code true} and the
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
                    : new UtilityElf.DefaultThreadFactory(poolName + ":network-timeout-executor", true);

            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);
            executor.setKeepAliveTime(15, TimeUnit.SECONDS);
            executor.allowCoreThreadTimeOut(true);
            netTimeoutExecutor = executor;
        }
    }

    /**
     * Set the loginTimeout on the specified DataSource.
     *
     * @param dataSource the DataSource
     */
    private void setLoginTimeout(CommonDataSource dataSource) {
        if (connectionTimeout != Integer.MAX_VALUE) {
            try {
                dataSource.setLoginTimeout(Math.max(1, (int) TimeUnit.MILLISECONDS.toSeconds(500L + connectionTimeout)));
            } catch (SQLException ex) {
                log.info("{} - Failed to set login timeout for data source. ({})", poolName, ex.getMessage());
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
    private static @NotNull String stringFromResetBits(int bits) {
        StringBuilder sb = new StringBuilder();
        int size = RESET_STATES.length;

        for (int ndx = 0; ndx < size; ndx++) {
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

        private void writeObject(@NotNull ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();
        }

        private void readObject(@NotNull ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
        }
    }

    /**
     * Special executor used only to work around a MySQL issue that has not been addressed.
     * MySQL issue: <a href="http://bugs.mysql.com/bug.php?id=75615">MySQL Bug Report</a>
     */
    @NoArgsConstructor
    private static class SynchronousExecutor implements Executor {

        @Override
        public void execute(@NotNull Runnable command) {
            try {
                command.run();
            } catch (RuntimeException ex) {
                log.debug("Failed to execute: {}", command, ex);
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
            tracker.recordConnectionAcquiredNanos(ClockSource.elapsedNanos(startTime));
        }

        @Override
        public void recordBorrowStats(@NotNull PoolEntry poolEntry, long startTime) {
            long now = ClockSource.currentTime();
            poolEntry.lastBorrowed = now;
            tracker.recordConnectionAcquiredNanos(ClockSource.elapsedNanos(startTime, now));
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
    @NoArgsConstructor
    static final class NopMetricsTrackerDelegate implements IMetricsTrackerDelegate {

    }
}
