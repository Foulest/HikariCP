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
package com.zaxxer.hikari.metrics;

import com.zaxxer.hikari.util.ClockSource;

import java.util.concurrent.atomic.AtomicLong;

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

    protected PoolStats(long timeoutMs) {
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
        while (true) {
            long now = ClockSource.currentTime();
            long reloadTime = reloadAt.get();

            if (reloadTime > now) {
                return false;
            } else if (reloadAt.compareAndSet(reloadTime, ClockSource.plusMillis(now, timeoutMs))) {
                return true;
            }
        }
    }
}
