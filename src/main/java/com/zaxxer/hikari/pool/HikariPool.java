package com.zaxxer.hikari.pool;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleHealthChecker;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleMetricsTrackerFactory;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import com.zaxxer.hikari.util.ConcurrentBag;
import com.zaxxer.hikari.util.ConcurrentBag.IBagStateListener;
import com.zaxxer.hikari.util.SuspendResumeLock;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static com.zaxxer.hikari.util.ClockSource.*;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static com.zaxxer.hikari.util.UtilityElf.*;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This is the primary connection pool class that provides the basic
 * pooling behavior for HikariCP.
 *
 * @author Brett Wooldridge
 */
@Slf4j
@SuppressWarnings("unused")
public final class HikariPool extends PoolBase implements HikariPoolMXBean, IBagStateListener {

    public static final int POOL_NORMAL = 0;
    public static final int POOL_SUSPENDED = 1;
    public static final int POOL_SHUTDOWN = 2;

    public volatile int poolState;

    private final long aliveBypassWindowMs = Long.getLong("com.zaxxer.hikari.aliveBypassWindowMs", MILLISECONDS.toMillis(500));
    private final long housekeepingPeriodMs = Long.getLong("com.zaxxer.hikari.housekeeping.periodMs", SECONDS.toMillis(30));

    private static final String EVICTED_CONNECTION_MESSAGE = "(connection was evicted)";
    private static final String DEAD_CONNECTION_MESSAGE = "(connection is dead)";

    private final PoolEntryCreator poolEntryCreator = new PoolEntryCreator(null /*logging prefix*/);
    private final PoolEntryCreator postFillPoolEntryCreator = new PoolEntryCreator("After adding ");
    private final Collection<Runnable> addConnectionQueueReadOnlyView;
    private final ThreadPoolExecutor addConnectionExecutor;
    private final ThreadPoolExecutor closeConnectionExecutor;
    private final ConcurrentBag<PoolEntry> connectionBag;
    private final ProxyLeakTaskFactory leakTaskFactory;
    private final SuspendResumeLock suspendResumeLock;
    private final ScheduledExecutorService houseKeepingExecutorService;

    private ScheduledFuture<?> houseKeeperTask;

    /**
     * Construct a HikariPool with the specified configuration.
     *
     * @param config a HikariConfig instance
     */
    public HikariPool(HikariConfig config) {
        super(config);
        connectionBag = new ConcurrentBag<>(this);
        suspendResumeLock = config.isAllowPoolSuspension() ? new SuspendResumeLock() : SuspendResumeLock.FAUX_LOCK;
        houseKeepingExecutorService = initializeHouseKeepingExecutorService();

        checkFailFast();

        if (config.getMetricsTrackerFactory() != null) {
            setMetricsTrackerFactory(config.getMetricsTrackerFactory());
        } else {
            setMetricRegistry(config.getMetricRegistry());
        }

        setHealthCheckRegistry(config.getHealthCheckRegistry());
        handleMBeans(this, true);

        ThreadFactory threadFactory = config.getThreadFactory();
        int maxPoolSize = config.getMaximumPoolSize();
        LinkedBlockingQueue<Runnable> addConnectionQueue = new LinkedBlockingQueue<>(maxPoolSize);
        addConnectionQueueReadOnlyView = unmodifiableCollection(addConnectionQueue);

        addConnectionExecutor = createThreadPoolExecutor(addConnectionQueue,
                poolName + " connection adder", threadFactory,
                new ThreadPoolExecutor.DiscardOldestPolicy());

        closeConnectionExecutor = createThreadPoolExecutor(maxPoolSize,
                poolName + " connection closer", threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy());

        leakTaskFactory = new ProxyLeakTaskFactory(config.getLeakDetectionThreshold(), houseKeepingExecutorService);

        houseKeeperTask = houseKeepingExecutorService.scheduleWithFixedDelay(new HouseKeeper(),
                100L, housekeepingPeriodMs, MILLISECONDS);

        if (Boolean.getBoolean("com.zaxxer.hikari.blockUntilFilled")
                && config.getInitializationFailTimeout() > 1) {
            addConnectionExecutor.setMaximumPoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));
            addConnectionExecutor.setCorePoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));

            while (elapsedMillis(currentTime()) < config.getInitializationFailTimeout()
                    && getTotalConnections() < config.getMinimumIdle()) {
                quietlySleep(MILLISECONDS.toMillis(100));
            }

            addConnectionExecutor.setCorePoolSize(1);
            addConnectionExecutor.setMaximumPoolSize(1);
        }
    }

    /**
     * Get a connection from the pool, or timeout after connectionTimeout milliseconds.
     *
     * @return a java.sql.Connection instance
     * @throws SQLException thrown if a timeout occurs trying to obtain a connection
     */
    public Connection getConnection() throws SQLException {
        return getConnection(connectionTimeout);
    }

    /**
     * Get a connection from the pool, or timeout after the specified number of milliseconds.
     *
     * @param hardTimeout the maximum time to wait for a connection from the pool
     * @return a java.sql.Connection instance
     * @throws SQLException thrown if a timeout occurs trying to obtain a connection
     */
    public Connection getConnection(long hardTimeout) throws SQLException {
        suspendResumeLock.acquire();
        long startTime = currentTime();

        try {
            long timeout = hardTimeout;

            do {
                PoolEntry poolEntry = connectionBag.borrow(timeout, MILLISECONDS);

                if (poolEntry == null) {
                    break; // We timed out... break and throw exception
                }

                long now = currentTime();

                if (poolEntry.isMarkedEvicted()
                        || (elapsedMillis(poolEntry.lastAccessed, now) > aliveBypassWindowMs
                        && !isConnectionAlive(poolEntry.connection))) {
                    closeConnection(poolEntry, poolEntry.isMarkedEvicted()
                            ? EVICTED_CONNECTION_MESSAGE : DEAD_CONNECTION_MESSAGE);
                    timeout = hardTimeout - elapsedMillis(startTime);
                } else {
                    metricsTracker.recordBorrowStats(poolEntry, startTime);
                    return poolEntry.createProxyConnection(leakTaskFactory.schedule(poolEntry), now);
                }
            } while (timeout > 0L);
            metricsTracker.recordBorrowTimeoutStats(startTime);
            throw createTimeoutException(startTime);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new SQLException(poolName + " - Interrupted during connection acquisition", ex);
        } finally {
            suspendResumeLock.release();
        }
    }

    /**
     * Shutdown the pool, closing all idle connections and aborting or closing
     * active connections.
     *
     * @throws InterruptedException thrown if the thread is interrupted during shutdown
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public synchronized void shutdown() throws InterruptedException {
        try {
            poolState = POOL_SHUTDOWN;

            if (addConnectionExecutor == null) { // pool never started
                return;
            }

            logPoolState("Before shutdown ");

            if (houseKeeperTask != null) {
                houseKeeperTask.cancel(false);
                houseKeeperTask = null;
            }

            softEvictConnections();

            addConnectionExecutor.shutdown();
            addConnectionExecutor.awaitTermination(getLoginTimeout(), SECONDS);

            destroyHouseKeepingExecutorService();

            connectionBag.close();

            ExecutorService assassinExecutor = createThreadPoolExecutor(config.getMaximumPoolSize(),
                    poolName + " connection assassinator",
                    config.getThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

            try {
                long start = currentTime();
                do {
                    abortActiveConnections(assassinExecutor);
                    softEvictConnections();
                } while (getTotalConnections() > 0 && elapsedMillis(start) < SECONDS.toMillis(10));
            } finally {
                assassinExecutor.shutdown();
                assassinExecutor.awaitTermination(10L, SECONDS);
            }

            shutdownNetworkTimeoutExecutor();
            closeConnectionExecutor.shutdown();
            closeConnectionExecutor.awaitTermination(10L, SECONDS);
        } finally {
            logPoolState("After shutdown ");
            handleMBeans(this, false);
            metricsTracker.close();
        }
    }

    /**
     * Evict a Connection from the pool.
     *
     * @param connection the Connection to evict (actually a {@link ProxyConnection})
     */
    public void evictConnection(@NotNull Connection connection) {
        ProxyConnection proxyConnection = (ProxyConnection) connection;
        proxyConnection.cancelLeakTask();

        try {
            softEvictConnection(proxyConnection.getPoolEntry(),
                    "(connection evicted by user)", !connection.isClosed() /* owner */);
        } catch (SQLException ignored) {
        }
    }

    /**
     * Set a metrics registry to be used when registering metrics collectors.  The HikariDataSource prevents this
     * method from being called more than once.
     *
     * @param metricRegistry the metrics registry instance to use
     */
    public void setMetricRegistry(Object metricRegistry) {
        if (metricRegistry != null
                && safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")) {
            setMetricsTrackerFactory(new CodahaleMetricsTrackerFactory((MetricRegistry) metricRegistry));

        } else if (metricRegistry != null
                && safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry")) {
            setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory((MeterRegistry) metricRegistry));

        } else {
            setMetricsTrackerFactory(null);
        }
    }

    /**
     * Set the MetricsTrackerFactory to be used to create the IMetricsTracker instance used by the pool.
     *
     * @param metricsTrackerFactory an instance of a class that subclasses MetricsTrackerFactory
     */
    public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
        if (metricsTrackerFactory != null) {
            metricsTracker = new MetricsTrackerDelegate(
                    metricsTrackerFactory.create(config.getPoolName(), getPoolStats())
            );
        } else {
            metricsTracker = new NopMetricsTrackerDelegate();
        }
    }

    /**
     * Set the health check registry to be used when registering health checks.  Currently only Codahale health
     * checks are supported.
     *
     * @param healthCheckRegistry the health check registry instance to use
     */
    public void setHealthCheckRegistry(Object healthCheckRegistry) {
        if (healthCheckRegistry != null) {
            CodahaleHealthChecker.registerHealthChecks(this, config, (HealthCheckRegistry) healthCheckRegistry);
        }
    }

    // ***********************************************************************
    //                        IBagStateListener callback
    // ***********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBagItem(int waiting) {
        boolean shouldAdd = waiting - addConnectionQueueReadOnlyView.size() >= 0; // Yes, >= is intentional.

        if (shouldAdd) {
            addConnectionExecutor.submit(poolEntryCreator);
        } else {
            log.debug("{} - Add connection elided, waiting {}, queue {}",
                    poolName, waiting, addConnectionQueueReadOnlyView.size());
        }
    }

    // ***********************************************************************
    //                        HikariPoolMBean methods
    // ***********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public int getActiveConnections() {
        return connectionBag.getCount(STATE_IN_USE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getIdleConnections() {
        return connectionBag.getCount(STATE_NOT_IN_USE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTotalConnections() {
        return connectionBag.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getThreadsAwaitingConnection() {
        return connectionBag.getWaitingThreadCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void softEvictConnections() {
        connectionBag.values().forEach(poolEntry -> softEvictConnection(poolEntry,
                "(connection evicted)", false));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void suspendPool() {
        if (suspendResumeLock == SuspendResumeLock.FAUX_LOCK) {
            throw new IllegalStateException(poolName + " - is not suspendable");
        } else if (poolState != POOL_SUSPENDED) {
            suspendResumeLock.suspend();
            poolState = POOL_SUSPENDED;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void resumePool() {
        if (poolState == POOL_SUSPENDED) {
            poolState = POOL_NORMAL;
            fillPool();
            suspendResumeLock.resume();
        }
    }

    // ***********************************************************************
    //                           Package methods
    // ***********************************************************************

    /**
     * Log the current pool state at debug level.
     *
     * @param prefix an optional prefix to prepend the log message
     */
    void logPoolState(String... prefix) {
        if (log.isDebugEnabled()) {
            log.debug("{} - {}stats (total={}, active={}, idle={}, waiting={})",
                    poolName, (prefix.length > 0 ? prefix[0] : ""), getTotalConnections(),
                    getActiveConnections(), getIdleConnections(), getThreadsAwaitingConnection());
        }
    }

    /**
     * Recycle PoolEntry (add back to the pool)
     *
     * @param poolEntry the PoolEntry to recycle
     */
    @Override
    void recycle(PoolEntry poolEntry) {
        metricsTracker.recordConnectionUsage(poolEntry);
        connectionBag.requite(poolEntry);
    }

    /**
     * Permanently close the real (underlying) connection (eat any exception).
     *
     * @param poolEntry     poolEntry having the connection to close
     * @param closureReason reason to close
     */
    void closeConnection(PoolEntry poolEntry, String closureReason) {
        if (connectionBag.remove(poolEntry)) {
            Connection connection = poolEntry.close();

            closeConnectionExecutor.execute(() -> {
                quietlyCloseConnection(connection, closureReason);

                if (poolState == POOL_NORMAL) {
                    fillPool();
                }
            });
        }
    }

    int[] getPoolStateCounts() {
        return connectionBag.getStateCounts();
    }

    // ***********************************************************************
    //                           Private methods
    // ***********************************************************************

    /**
     * Creating new poolEntry.  If maxLifetime is configured, create a future End-of-life task with 2.5% variance from
     * the maxLifetime time to ensure there is no massive die-off of Connections in the pool.
     */
    private @Nullable PoolEntry createPoolEntry() {
        try {
            PoolEntry poolEntry = newPoolEntry();
            long maxLifetime = config.getMaxLifetime();

            if (maxLifetime > 0) {
                // variance up to 2.5% of the maxLifetime
                long variance = maxLifetime > 10_000 ? ThreadLocalRandom.current().nextLong(maxLifetime / 40) : 0;
                long lifetime = maxLifetime - variance;

                poolEntry.setFutureEol(houseKeepingExecutorService.schedule(
                        new MaxLifetimeTask(poolEntry), lifetime, MILLISECONDS)
                );
            }

            long keepaliveTime = config.getKeepaliveTime();

            if (keepaliveTime > 0) {
                // variance up to 10% of the heartbeat time
                long variance = ThreadLocalRandom.current().nextLong(keepaliveTime / 10);
                long heartbeatTime = keepaliveTime - variance;

                poolEntry.setKeepalive(houseKeepingExecutorService.scheduleWithFixedDelay(
                        new KeepaliveTask(poolEntry), heartbeatTime, heartbeatTime, MILLISECONDS));
            }
            return poolEntry;

        } catch (ConnectionSetupException ex) {
            // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
            if (poolState == POOL_NORMAL) {
                log.error("{} - Error thrown while acquiring connection from data source", poolName, ex.getCause());
                lastConnectionFailure.set(ex);
            }

        } catch (Exception ex) {
            // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
            if (poolState == POOL_NORMAL) {
                log.debug("{} - Cannot acquire connection from data source", poolName, ex);
            }
        }
        return null;
    }

    /**
     * Fill pool up from current idle connections (as they are perceived
     * at the point of execution) to minimumIdle connections.
     */
    private synchronized void fillPool() {
        int connectionsToAdd = Math.min(config.getMaximumPoolSize() - getTotalConnections(),
                config.getMinimumIdle() - getIdleConnections()) - addConnectionQueueReadOnlyView.size();

        if (connectionsToAdd <= 0) {
            log.debug("{} - Fill pool skipped, pool is at sufficient level.", poolName);
        }

        for (int i = 0; i < connectionsToAdd; i++) {
            addConnectionExecutor.submit((i < connectionsToAdd - 1) ? poolEntryCreator : postFillPoolEntryCreator);
        }
    }

    /**
     * Attempt to abort or close active connections.
     *
     * @param assassinExecutor the ExecutorService to pass to Connection.abort()
     */
    private void abortActiveConnections(ExecutorService assassinExecutor) {
        for (PoolEntry poolEntry : connectionBag.values(STATE_IN_USE)) {
            Connection connection = poolEntry.close();

            try {
                connection.abort(assassinExecutor);
            } catch (Throwable ex) {
                quietlyCloseConnection(connection, "(connection aborted during shutdown)");
            } finally {
                connectionBag.remove(poolEntry);
            }
        }
    }

    /**
     * If initializationFailFast is configured, check that we have DB connectivity.
     *
     * @throws PoolInitializationException if fails to create or validate connection
     * @see HikariConfig#setInitializationFailTimeout(long)
     */
    private void checkFailFast() {
        long initializationTimeout = config.getInitializationFailTimeout();

        if (initializationTimeout < 0) {
            return;
        }

        long startTime = currentTime();

        do {
            PoolEntry poolEntry = createPoolEntry();

            if (poolEntry != null) {
                if (config.getMinimumIdle() > 0) {
                    connectionBag.add(poolEntry);
                    log.debug("{} - Added connection {}", poolName, poolEntry.connection);
                } else {
                    quietlyCloseConnection(poolEntry.close(),
                            "(initialization check complete and minimumIdle is zero)");
                }
                return;
            }

            if (getLastConnectionFailure() instanceof ConnectionSetupException) {
                throwPoolInitializationException(getLastConnectionFailure().getCause());
            }

            quietlySleep(SECONDS.toMillis(1));
        } while (elapsedMillis(startTime) < initializationTimeout);

        if (initializationTimeout > 0) {
            throwPoolInitializationException(getLastConnectionFailure());
        }
    }

    /**
     * Log the Throwable that caused pool initialization to fail, and then throw a PoolInitializationException with
     * that cause attached.
     *
     * @param ex the Throwable that caused the pool to fail to initialize (possibly null)
     */
    private void throwPoolInitializationException(Throwable ex) {
        log.error("{} - Exception during pool initialization.", poolName, ex);
        destroyHouseKeepingExecutorService();
        throw new PoolInitializationException(ex);
    }

    /**
     * "Soft" evict a Connection (/PoolEntry) from the pool.  If this method is being called by the user directly
     * through {@link com.zaxxer.hikari.HikariDataSource#evictConnection(Connection)} then {@code owner} is {@code true}.
     * <p>
     * If the caller is the owner, or if the Connection is idle (i.e. can be "reserved" in the {@link ConcurrentBag}),
     * then we can close the connection immediately.  Otherwise, we leave it "marked" for eviction so that it is evicted
     * the next time someone tries to acquire it from the pool.
     *
     * @param poolEntry the PoolEntry (/Connection) to "soft" evict from the pool
     * @param reason    the reason that the connection is being evicted
     * @param owner     true if the caller is the owner of the connection, false otherwise
     * @return true if the connection was evicted (closed), false if it was merely marked for eviction
     */
    private boolean softEvictConnection(@NotNull PoolEntry poolEntry, String reason, boolean owner) {
        poolEntry.markEvicted();

        if (owner || connectionBag.reserve(poolEntry)) {
            closeConnection(poolEntry, reason);
            return true;
        }
        return false;
    }

    /**
     * Create/initialize the Housekeeping service {@link ScheduledExecutorService}.  If the user specified an Executor
     * to be used in the {@link HikariConfig}, then we use that.  If no Executor was specified (typical), then create
     * an Executor and configure it.
     *
     * @return either the user specified {@link ScheduledExecutorService}, or the one we created
     */
    private ScheduledExecutorService initializeHouseKeepingExecutorService() {
        if (config.getScheduledExecutor() == null) {
            ThreadFactory threadFactory = Optional.ofNullable(config.getThreadFactory()).orElseGet(() ->
                    new DefaultThreadFactory(poolName + " housekeeper", true)
            );

            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
                    threadFactory, new ThreadPoolExecutor.DiscardPolicy());

            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        } else {
            return config.getScheduledExecutor();
        }
    }

    /**
     * Destroy (/shutdown) the Housekeeping service Executor, if it was the one that we created.
     */
    private void destroyHouseKeepingExecutorService() {
        if (config.getScheduledExecutor() == null) {
            houseKeepingExecutorService.shutdownNow();
        }
    }

    /**
     * Create a PoolStats instance that will be used by metrics tracking, with a pollable resolution of 1 second.
     *
     * @return a PoolStats instance
     */
    @Contract(" -> new")
    private @NotNull PoolStats getPoolStats() {
        return new PoolStats(SECONDS.toMillis(1)) {
            @Override
            protected void update() {
                pendingThreads = getThreadsAwaitingConnection();
                idleConnections = HikariPool.this.getIdleConnections();
                totalConnections = HikariPool.this.getTotalConnections();
                activeConnections = HikariPool.this.getActiveConnections();
                maxConnections = config.getMaximumPoolSize();
                minConnections = config.getMinimumIdle();
            }
        };
    }

    /**
     * Create a timeout exception (specifically, {@link SQLTransientConnectionException}) to be thrown, because a
     * timeout occurred when trying to acquire a Connection from the pool.
     * <p>
     * If there was an underlying cause for the timeout, e.g. a SQLException thrown by the driver while trying to
     * create a new Connection, then use the SQL State from that exception as our own and additionally set that
     * exception as the "next" SQLException inside our exception.
     * <p>
     * As a side effect, log the timeout failure at DEBUG, and record the timeout failure in the metrics tracker.
     *
     * @param startTime the start time (timestamp) of the acquisition attempt
     * @return a SQLException to be thrown from {@link #getConnection()}
     */
    private @NotNull SQLException createTimeoutException(long startTime) {
        logPoolState("Timeout failure ");
        metricsTracker.recordConnectionTimeout();

        String sqlState = null;
        Throwable originalException = getLastConnectionFailure();

        if (originalException instanceof SQLException) {
            sqlState = ((SQLException) originalException).getSQLState();
        }

        SQLException connectionException = new SQLTransientConnectionException(poolName
                + " - Connection is not available, request timed out after "
                + elapsedMillis(startTime) + "ms.", sqlState, originalException);

        if (originalException instanceof SQLException) {
            connectionException.setNextException((SQLException) originalException);
        }
        return connectionException;
    }

    // ***********************************************************************
    //                      Non-anonymous Inner-classes
    // ***********************************************************************

    /**
     * Creating and adding poolEntries (connections) to the pool.
     */
    @AllArgsConstructor
    private final class PoolEntryCreator implements Callable<Boolean> {

        private final String loggingPrefix;

        @Override
        public Boolean call() {
            long sleepBackoff = 250L;

            while (poolState == POOL_NORMAL && shouldCreateAnotherConnection()) {
                PoolEntry poolEntry = createPoolEntry();

                if (poolEntry != null) {
                    connectionBag.add(poolEntry);
                    log.debug("{} - Added connection {}", poolName, poolEntry.connection);

                    if (loggingPrefix != null) {
                        logPoolState(loggingPrefix);
                    }
                    return Boolean.TRUE;
                }

                // failed to get connection from db, sleep and retry
                if (loggingPrefix != null) {
                    log.debug("{} - Connection add failed, sleeping with backoff: {}ms", poolName, sleepBackoff);
                }

                quietlySleep(sleepBackoff);

                sleepBackoff = Math.min(SECONDS.toMillis(10),
                        Math.min(connectionTimeout, (long) (sleepBackoff * 1.5)));
            }

            // Pool is suspended or shutdown or at max size
            return Boolean.FALSE;
        }

        /**
         * We only create connections if we need another idle connection or have threads still waiting
         * for a new connection. Otherwise, we bail out of the request to create.
         *
         * @return true if we should create a connection, false if the need has disappeared
         */
        private synchronized boolean shouldCreateAnotherConnection() {
            return getTotalConnections() < config.getMaximumPoolSize() &&
                    (connectionBag.getWaitingThreadCount() > 0 || getIdleConnections() < config.getMinimumIdle());
        }
    }

    /**
     * The housekeeping task to retire and maintain minimum idle connections.
     */
    private final class HouseKeeper implements Runnable {

        private volatile long previous = plusMillis(currentTime(), -housekeepingPeriodMs);

        @Override
        @SuppressWarnings("NonAtomicOperationOnVolatileField")
        public void run() {
            try {
                // refresh values in case they changed via MBean
                connectionTimeout = config.getConnectionTimeout();
                validationTimeout = config.getValidationTimeout();
                leakTaskFactory.updateLeakDetectionThreshold(config.getLeakDetectionThreshold());

                catalog = (config.getCatalog() != null
                        && !config.getCatalog().equals(catalog)) ? config.getCatalog() : catalog;

                long idleTimeout = config.getIdleTimeout();
                long now = currentTime();

                // Detect retrograde time, allowing +128ms as per NTP spec.
                if (plusMillis(now, 128) < plusMillis(previous, housekeepingPeriodMs)) {
                    log.warn("{} - Retrograde clock change detected (housekeeper delta={}),"
                            + " soft-evicting connections from pool.", poolName, elapsedDisplayString(previous, now));
                    previous = now;
                    softEvictConnections();
                    return;
                } else if (now > plusMillis(previous, (3 * housekeepingPeriodMs) / 2)) {
                    // No point evicting for forward clock motion, this merely accelerates connection retirement anyway
                    log.warn("{} - Thread starvation or clock leap detected (housekeeper delta={}).",
                            poolName, elapsedDisplayString(previous, now));
                }

                previous = now;

                String afterPrefix = "Pool ";

                if (idleTimeout > 0L && config.getMinimumIdle() < config.getMaximumPoolSize()) {
                    logPoolState("Before cleanup ");
                    afterPrefix = "After cleanup  ";

                    List<PoolEntry> notInUse = connectionBag.values(STATE_NOT_IN_USE);
                    int toRemove = notInUse.size() - config.getMinimumIdle();

                    for (PoolEntry entry : notInUse) {
                        if (toRemove > 0 && connectionBag.reserve(entry)
                                && elapsedMillis(entry.lastAccessed, now) > idleTimeout) {
                            closeConnection(entry, "(connection has passed idleTimeout)");
                            toRemove--;
                        }
                    }
                }

                logPoolState(afterPrefix);
                fillPool(); // Try to maintain minimum connections
            } catch (Exception ex) {
                log.error("Unexpected exception in housekeeping task", ex);
            }
        }
    }

    @AllArgsConstructor
    private final class MaxLifetimeTask implements Runnable {

        private final PoolEntry poolEntry;

        public void run() {
            if (softEvictConnection(poolEntry, "(connection has passed maxLifetime)", false)) {
                addBagItem(connectionBag.getWaitingThreadCount());
            }
        }
    }

    @AllArgsConstructor
    private final class KeepaliveTask implements Runnable {

        private final PoolEntry poolEntry;

        public void run() {
            if (connectionBag.reserve(poolEntry)) {
                if (!isConnectionAlive(poolEntry.connection)) {
                    softEvictConnection(poolEntry, DEAD_CONNECTION_MESSAGE, true);
                    addBagItem(connectionBag.getWaitingThreadCount());
                } else {
                    connectionBag.unreserve(poolEntry);
                    log.debug("{} - keepalive: connection {} is alive", poolName, poolEntry.connection);
                }
            }
        }
    }

    public static class PoolInitializationException extends RuntimeException {

        private static final long serialVersionUID = 929872118275916520L;

        /**
         * Construct an exception, possibly wrapping the provided Throwable as the cause.
         *
         * @param ex the Throwable to wrap
         */
        public PoolInitializationException(Throwable ex) {
            super("Failed to initialize pool: " + ex.getMessage(), ex);
        }
    }
}
