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

import com.zaxxer.hikari.util.ClockSource;
import com.zaxxer.hikari.util.ConcurrentBag;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Entry used in the ConcurrentBag to track Connection instances.
 *
 * @author Brett Wooldridge
 */
@Slf4j
@Setter
@Getter(lombok.AccessLevel.PACKAGE)
final class PoolEntry implements ConcurrentBag.IConcurrentBagEntry {

    private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater;

    Connection connection;
    long lastAccessed;
    long lastBorrowed;

    private volatile int state;

    @Getter(lombok.AccessLevel.PACKAGE)
    private volatile boolean markedEvicted;

    private final AtomicReference<ScheduledFuture<?>> endOfLife;
    private final AtomicReference<ScheduledFuture<?>> keepalive;

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
        lastAccessed = ClockSource.currentTime();
        openStatements = new FastList<>(Statement.class, 16);
        endOfLife = new AtomicReference<>();
        keepalive = new AtomicReference<>();
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
        this.endOfLife.set(endOfLife);
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
        return ClockSource.elapsedMillis(lastBorrowed);
    }

    PoolBase getPoolBase() {
        return hikariPool;
    }

    @Override
    public @NotNull String toString() {
        return connection + ", accessed " + ClockSource.elapsedDisplayString(lastAccessed, ClockSource.currentTime()) + " ago, " + stateToString();
    }

    // ***********************************************************************
    //                      IConcurrentBagEntry methods
    // ***********************************************************************

    @Override
    public int getState() {
        return stateUpdater.get(this);
    }

    @Override
    public boolean compareAndSet(int expect, int update) {
        return stateUpdater.compareAndSet(this, expect, update);
    }

    @Override
    public void setState(int update) {
        stateUpdater.set(this, update);
    }

    Connection close() {
        ScheduledFuture<?> eol = endOfLife.getAndSet(null);

        if (eol != null && !eol.isDone() && !eol.cancel(false)) {
            log.warn("{} - maxLifeTime expiration task cancellation unexpectedly"
                    + " returned false for connection {}", getPoolName(), connection);
        }

        ScheduledFuture<?> ka = keepalive.getAndSet(null);

        if (ka != null && !ka.isDone() && !ka.cancel(false)) {
            log.warn("{} - keepalive task cancellation unexpectedly returned"
                    + " false for connection {}", getPoolName(), connection);
        }

        Connection con = connection;
        connection = null;
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
