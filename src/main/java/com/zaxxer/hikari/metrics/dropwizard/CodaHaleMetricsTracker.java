package com.zaxxer.hikari.metrics.dropwizard;

import com.codahale.metrics.*;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

@Getter
public final class CodaHaleMetricsTracker implements IMetricsTracker {

    private final String poolName;
    private final Timer connectionAcquisitionTimer;
    private final Histogram connectionDurationHistogram;
    private final Histogram connectionCreationHistogram;
    private final Meter connectionTimeoutMeter;
    private final MetricRegistry registry;

    private static final String METRIC_CATEGORY = "pool";
    private static final String METRIC_NAME_WAIT = "Wait";
    private static final String METRIC_NAME_USAGE = "Usage";
    private static final String METRIC_NAME_CONNECT = "ConnectionCreation";
    private static final String METRIC_NAME_TIMEOUT_RATE = "ConnectionTimeoutRate";
    private static final String METRIC_NAME_TOTAL_CONNECTIONS = "TotalConnections";
    private static final String METRIC_NAME_IDLE_CONNECTIONS = "IdleConnections";
    private static final String METRIC_NAME_ACTIVE_CONNECTIONS = "ActiveConnections";
    private static final String METRIC_NAME_PENDING_CONNECTIONS = "PendingConnections";
    private static final String METRIC_NAME_MAX_CONNECTIONS = "MaxConnections";
    private static final String METRIC_NAME_MIN_CONNECTIONS = "MinConnections";

    CodaHaleMetricsTracker(String poolName,
                           @NotNull PoolStats poolStats,
                           @NotNull MetricRegistry registry) {
        this.poolName = poolName;
        this.registry = registry;
        connectionAcquisitionTimer = registry.timer(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_WAIT));
        connectionDurationHistogram = registry.histogram(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_USAGE));
        connectionCreationHistogram = registry.histogram(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_CONNECT));
        connectionTimeoutMeter = registry.meter(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_TIMEOUT_RATE));

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_TOTAL_CONNECTIONS),
                (Gauge<Integer>) poolStats::getTotalConnections);

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_IDLE_CONNECTIONS),
                (Gauge<Integer>) poolStats::getIdleConnections);

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_ACTIVE_CONNECTIONS),
                (Gauge<Integer>) poolStats::getActiveConnections);

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_PENDING_CONNECTIONS),
                (Gauge<Integer>) poolStats::getPendingThreads);

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_MAX_CONNECTIONS),
                (Gauge<Integer>) poolStats::getMaxConnections);

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_MIN_CONNECTIONS),
                (Gauge<Integer>) poolStats::getMinConnections);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_WAIT));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_USAGE));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_CONNECT));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_TIMEOUT_RATE));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_TOTAL_CONNECTIONS));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_IDLE_CONNECTIONS));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_ACTIVE_CONNECTIONS));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_PENDING_CONNECTIONS));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_MAX_CONNECTIONS));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_MIN_CONNECTIONS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
        connectionAcquisitionTimer.update(elapsedAcquiredNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
        connectionDurationHistogram.update(elapsedBorrowedMillis);
    }

    @Override
    public void recordConnectionTimeout() {
        connectionTimeoutMeter.mark();
    }

    @Override
    public void recordConnectionCreatedMillis(long connectionCreatedMillis) {
        connectionCreationHistogram.update(connectionCreatedMillis);
    }
}
