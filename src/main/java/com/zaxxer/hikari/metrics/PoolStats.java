package com.zaxxer.hikari.metrics;

import java.util.concurrent.atomic.AtomicLong;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.plusMillis;

/**
 * @author Brett Wooldridge
 */
public abstract class PoolStats {

    private final AtomicLong reloadAt;
    private final long timeoutMs;

    protected volatile int totalConnections;
    protected volatile int idleConnections;
    protected volatile int activeConnections;
    protected volatile int pendingThreads;
    protected volatile int maxConnections;
    protected volatile int minConnections;

    public PoolStats(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        reloadAt = new AtomicLong();
    }

    public int getTotalConnections() {
        if (shouldLoad()) {
            update();
        }
        return totalConnections;
    }

    public int getIdleConnections() {
        if (shouldLoad()) {
            update();
        }
        return idleConnections;
    }

    public int getActiveConnections() {
        if (shouldLoad()) {
            update();
        }
        return activeConnections;
    }

    public int getPendingThreads() {
        if (shouldLoad()) {
            update();
        }
        return pendingThreads;
    }

    public int getMaxConnections() {
        if (shouldLoad()) {
            update();
        }
        return maxConnections;
    }

    public int getMinConnections() {
        if (shouldLoad()) {
            update();
        }
        return minConnections;
    }

    protected abstract void update();

    private boolean shouldLoad() {
        for (; ; ) {
            long now = currentTime();
            long reloadTime = reloadAt.get();

            if (reloadTime > now) {
                return false;
            } else if (reloadAt.compareAndSet(reloadTime, plusMillis(now, timeoutMs))) {
                return true;
            }
        }
    }
}
