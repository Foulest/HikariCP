package com.zaxxer.hikari.pool;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Runnable that is scheduled in the future to report leaks.  The ScheduledFuture is
 * cancelled if the connection is closed before the leak time expires.
 *
 * @author Brett Wooldridge
 */
class ProxyLeakTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyLeakTask.class);
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
            }

            @Override
            public void run() {
            }

            @Override
            public void cancel() {
            }
        };
    }

    ProxyLeakTask(@NotNull PoolEntry poolEntry) {
        exception = new Exception("Apparent connection leak detected");
        threadName = Thread.currentThread().getName();
        connectionName = poolEntry.connection.toString();
    }

    private ProxyLeakTask() {
    }

    void schedule(@NotNull ScheduledExecutorService executorService, long leakDetectionThreshold) {
        scheduledFuture = executorService.schedule(this, leakDetectionThreshold, TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        isLeaked = true;

        StackTraceElement[] stackTrace = exception.getStackTrace();
        StackTraceElement[] trace = new StackTraceElement[stackTrace.length - 5];
        System.arraycopy(stackTrace, 5, trace, 0, trace.length);

        exception.setStackTrace(trace);
        LOGGER.warn("Connection leak detection triggered for {} on thread {},"
                + " stack trace follows", connectionName, threadName, exception);
    }

    void cancel() {
        scheduledFuture.cancel(false);

        if (isLeaked) {
            LOGGER.info("Previously reported leaked connection {} on thread"
                    + " {} was returned to the pool (unleaked)", connectionName, threadName);
        }
    }
}
