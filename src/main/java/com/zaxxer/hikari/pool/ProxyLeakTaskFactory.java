package com.zaxxer.hikari.pool;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ScheduledExecutorService;

/**
 * A factory for {@link ProxyLeakTask} Runnables that are scheduled in the future to report leaks.
 *
 * @author Brett Wooldridge
 * @author Andreas Brenk
 */
class ProxyLeakTaskFactory {

    private final ScheduledExecutorService executorService;
    private long leakDetectionThreshold;

    ProxyLeakTaskFactory(long leakDetectionThreshold, ScheduledExecutorService executorService) {
        this.executorService = executorService;
        this.leakDetectionThreshold = leakDetectionThreshold;
    }

    ProxyLeakTask schedule(PoolEntry poolEntry) {
        return (leakDetectionThreshold == 0) ? ProxyLeakTask.NO_LEAK : scheduleNewTask(poolEntry);
    }

    void updateLeakDetectionThreshold(long leakDetectionThreshold) {
        this.leakDetectionThreshold = leakDetectionThreshold;
    }

    private @NotNull ProxyLeakTask scheduleNewTask(PoolEntry poolEntry) {
        ProxyLeakTask task = new ProxyLeakTask(poolEntry);
        task.schedule(executorService, leakDetectionThreshold);
        return task;
    }
}
