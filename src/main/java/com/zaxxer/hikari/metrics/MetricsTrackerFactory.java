package com.zaxxer.hikari.metrics;

public interface MetricsTrackerFactory {

    /**
     * Create an instance of an IMetricsTracker.
     *
     * @param poolName  the name of the pool
     * @param poolStats a PoolStats instance to use
     * @return a IMetricsTracker implementation instance
     */
    IMetricsTracker create(String poolName, PoolStats poolStats);
}
