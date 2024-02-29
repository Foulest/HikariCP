package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import com.zaxxer.hikari.util.FastList;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.zaxxer.hikari.util.ClockSource.*;

/**
 * Entry used in the ConcurrentBag to track Connection instances.
 *
 * @author Brett Wooldridge
 */
@Slf4j
@Setter
@Getter(lombok.AccessLevel.PACKAGE)
final class PoolEntry implements IConcurrentBagEntry {

    private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater;

    Connection connection;
    long lastAccessed;
    long lastBorrowed;

    private volatile int state = 0;
    @lombok.Getter(lombok.AccessLevel.PACKAGE)
    private volatile boolean markedEvicted;

    private volatile ScheduledFuture<?> endOfLife;
    private volatile ScheduledFuture<?> keepalive;

    private final FastList<Statement> openStatements;
    private final HikariPool hikariPool;

    private final boolean isReadOnly;
    private final boolean isAutoCommit;

    static {
        stateUpdater = AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");
    }

    PoolEntry(Connection connection, PoolBase pool, boolean isReadOnly, boolean isAutoCommit) {
        this.connection = connection;
        hikariPool = (HikariPool) pool;
        this.isReadOnly = isReadOnly;
        this.isAutoCommit = isAutoCommit;
        lastAccessed = currentTime();
        openStatements = new FastList<>(Statement.class, 16);
    }

    /**
     * Release this entry back to the pool.
     *
     * @param lastAccessed last access time-stamp
     */
    void recycle(long lastAccessed) {
        if (connection != null) {
            this.lastAccessed = lastAccessed;
            hikariPool.recycle(this);
        }
    }

    /**
     * Set the end of life {@link ScheduledFuture}.
     *
     * @param endOfLife this PoolEntry/Connection's end of life {@link ScheduledFuture}
     */
    void setFutureEol(ScheduledFuture<?> endOfLife) {
        this.endOfLife = endOfLife;
    }

    Connection createProxyConnection(ProxyLeakTask leakTask, long now) {
        return ProxyFactory.getProxyConnection(this, connection,
                openStatements, leakTask, now, isReadOnly, isAutoCommit);
    }

    void resetConnectionState(ProxyConnection proxyConnection, int dirtyBits) throws SQLException {
        hikariPool.resetConnectionState(connection, proxyConnection, dirtyBits);
    }

    String getPoolName() {
        return hikariPool.toString();
    }

    void markEvicted() {
        markedEvicted = true;
    }

    void evict(String closureReason) {
        hikariPool.closeConnection(this, closureReason);
    }

    /**
     * Returns millis since lastBorrowed
     */
    long getMillisSinceBorrowed() {
        return elapsedMillis(lastBorrowed);
    }

    PoolBase getPoolBase() {
        return hikariPool;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @NotNull String toString() {
        return connection + ", accessed " + elapsedDisplayString(lastAccessed, currentTime()) + " ago, " + stateToString();
    }

    // ***********************************************************************
    //                      IConcurrentBagEntry methods
    // ***********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public int getState() {
        return stateUpdater.get(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean compareAndSet(int expect, int update) {
        return stateUpdater.compareAndSet(this, expect, update);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setState(int update) {
        stateUpdater.set(this, update);
    }

    Connection close() {
        ScheduledFuture<?> eol = endOfLife;

        if (eol != null && !eol.isDone() && !eol.cancel(false)) {
            log.warn("{} - maxLifeTime expiration task cancellation unexpectedly"
                    + " returned false for connection {}", getPoolName(), connection);
        }

        ScheduledFuture<?> ka = keepalive;

        if (ka != null && !ka.isDone() && !ka.cancel(false)) {
            log.warn("{} - keepalive task cancellation unexpectedly returned"
                    + " false for connection {}", getPoolName(), connection);
        }

        Connection con = connection;
        connection = null;
        endOfLife = null;
        keepalive = null;
        return con;
    }

    private @NotNull String stateToString() {
        switch (state) {
            case STATE_IN_USE:
                return "IN_USE";
            case STATE_NOT_IN_USE:
                return "NOT_IN_USE";
            case STATE_REMOVED:
                return "REMOVED";
            case STATE_RESERVED:
                return "RESERVED";
            default:
                return "Invalid";
        }
    }
}
