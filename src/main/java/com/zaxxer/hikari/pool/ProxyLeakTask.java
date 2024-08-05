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

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Runnable that is scheduled in the future to report leaks.  The ScheduledFuture is
 * cancelled if the connection is closed before the leak time expires.
 *
 * @author Brett Wooldridge
 */
@Slf4j
@NoArgsConstructor
class ProxyLeakTask implements Runnable {

    static final ProxyLeakTask NO_LEAK;

    private ScheduledFuture<?> scheduledFuture;
    private String connectionName;
    private Exception exception;
    private String threadName;
    private boolean isLeaked;

    static {
        NO_LEAK = new ProxyLeakTask() {
            @Override
            void schedule(@NotNull ScheduledExecutorService executorService, long leakDetectionThreshold) {
                // Do nothing
            }

            @Override
            public void run() {
                // Do nothing
            }

            @Override
            public void cancel() {
                // Do nothing
            }
        };
    }

    ProxyLeakTask(@NotNull PoolEntry poolEntry) {
        exception = new Exception("Apparent connection leak detected");
        threadName = Thread.currentThread().getName();
        connectionName = poolEntry.connection.toString();
    }

    void schedule(@NotNull ScheduledExecutorService executorService, long leakDetectionThreshold) {
        scheduledFuture = executorService.schedule(this, leakDetectionThreshold, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        isLeaked = true;

        StackTraceElement[] stackTrace = exception.getStackTrace();
        StackTraceElement[] trace = new StackTraceElement[stackTrace.length - 5];
        System.arraycopy(stackTrace, 5, trace, 0, trace.length);

        exception.setStackTrace(trace);
        log.warn("Connection leak detection triggered for {} on thread {},"
                + " stack trace follows", connectionName, threadName, exception);
    }

    void cancel() {
        scheduledFuture.cancel(false);

        if (isLeaked) {
            log.info("Previously reported leaked connection {} on thread"
                    + " {} was returned to the pool (unleaked)", connectionName, threadName);
        }
    }
}
