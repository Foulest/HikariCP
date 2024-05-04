package com.zaxxer.hikari.metrics.micrometer;

import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

/**
 * {@link IMetricsTracker Metrics tracker} for Micrometer.
 * HikariCP metrics can be configured in your application by applying a
 * {@link io.micrometer.core.instrument.config.MeterFilter MeterFilter} to metrics starting with
 * {@link #HIKARI_METRIC_NAME_PREFIX}. For example, to configure client-side calculated percentiles:
 *
 * <blockquote><pre>
 *     new MeterFilter() {
 *       &#064;Override
 *       public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
 *         if (id.getName().startsWith(MicrometerMetricsTracker.HIKARI_METRIC_NAME_PREFIX)) {
 *           return DistributionStatisticConfig.builder()
 *               .percentiles(0.5, 0.95)
 *               .build()
 *               .merge(config);
 *            }
 *         return config;
 *         }
 *      };
 * </pre></blockquote>
 */
@Getter
public class MicrometerMetricsTracker implements IMetricsTracker {

    /**
     * Prefix used for all HikariCP metric names.
     */
    public static final String HIKARI_METRIC_NAME_PREFIX = "hikaricp";

    private static final String METRIC_CATEGORY = "pool";
    private static final String METRIC_NAME_WAIT = HIKARI_METRIC_NAME_PREFIX + ".connections.acquire";
    private static final String METRIC_NAME_USAGE = HIKARI_METRIC_NAME_PREFIX + ".connections.usage";
    private static final String METRIC_NAME_CONNECT = HIKARI_METRIC_NAME_PREFIX + ".connections.creation";

    private static final String METRIC_NAME_TIMEOUT_RATE = HIKARI_METRIC_NAME_PREFIX + ".connections.timeout";
    private static final String METRIC_NAME_TOTAL_CONNECTIONS = HIKARI_METRIC_NAME_PREFIX + ".connections";
    private static final String METRIC_NAME_IDLE_CONNECTIONS = HIKARI_METRIC_NAME_PREFIX + ".connections.idle";
    private static final String METRIC_NAME_ACTIVE_CONNECTIONS = HIKARI_METRIC_NAME_PREFIX + ".connections.active";
    private static final String METRIC_NAME_PENDING_CONNECTIONS = HIKARI_METRIC_NAME_PREFIX + ".connections.pending";
    private static final String METRIC_NAME_MAX_CONNECTIONS = HIKARI_METRIC_NAME_PREFIX + ".connections.max";
    private static final String METRIC_NAME_MIN_CONNECTIONS = HIKARI_METRIC_NAME_PREFIX + ".connections.min";

    private final Timer connectionObtainTimer;
    private final Counter connectionTimeoutCounter;
    private final Timer connectionUsage;
    private final Timer connectionCreation;
    private final Gauge totalConnectionGauge;
    private final Gauge idleConnectionGauge;
    private final Gauge activeConnectionGauge;
    private final Gauge pendingConnectionGauge;
    private final Gauge maxConnectionGauge;
    private final Gauge minConnectionGauge;
    private final MeterRegistry meterRegistry;
    private final PoolStats poolStats;

    MicrometerMetricsTracker(String poolName, PoolStats poolStats, MeterRegistry meterRegistry) {
        // poolStats must be held with a 'strong reference' even though it is never referenced within this class
        this.poolStats = poolStats; // DO NOT REMOVE

        this.meterRegistry = meterRegistry;

        connectionObtainTimer = Timer.builder(METRIC_NAME_WAIT)
                .description("Connection acquire time")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        connectionCreation = Timer.builder(METRIC_NAME_CONNECT)
                .description("Connection creation time")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        connectionUsage = Timer.builder(METRIC_NAME_USAGE)
                .description("Connection usage time")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        connectionTimeoutCounter = Counter.builder(METRIC_NAME_TIMEOUT_RATE)
                .description("Connection timeout total count")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        totalConnectionGauge = Gauge.builder(METRIC_NAME_TOTAL_CONNECTIONS, poolStats, PoolStats::getTotalConnections)
                .description("Total connections")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        idleConnectionGauge = Gauge.builder(METRIC_NAME_IDLE_CONNECTIONS, poolStats, PoolStats::getIdleConnections)
                .description("Idle connections")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        activeConnectionGauge = Gauge.builder(METRIC_NAME_ACTIVE_CONNECTIONS, poolStats, PoolStats::getActiveConnections)
                .description("Active connections")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        pendingConnectionGauge = Gauge.builder(METRIC_NAME_PENDING_CONNECTIONS, poolStats, PoolStats::getPendingThreads)
                .description("Pending threads")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        maxConnectionGauge = Gauge.builder(METRIC_NAME_MAX_CONNECTIONS, poolStats, PoolStats::getMaxConnections)
                .description("Max connections")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);

        minConnectionGauge = Gauge.builder(METRIC_NAME_MIN_CONNECTIONS, poolStats, PoolStats::getMinConnections)
                .description("Min connections")
                .tags(METRIC_CATEGORY, poolName)
                .register(meterRegistry);
    }

    @Override
    public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
        connectionObtainTimer.record(elapsedAcquiredNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
        connectionUsage.record(elapsedBorrowedMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void recordConnectionTimeout() {
        connectionTimeoutCounter.increment();
    }

    @Override
    public void recordConnectionCreatedMillis(long connectionCreatedMillis) {
        connectionCreation.record(connectionCreatedMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        meterRegistry.remove(connectionObtainTimer);
        meterRegistry.remove(connectionTimeoutCounter);
        meterRegistry.remove(connectionUsage);
        meterRegistry.remove(connectionCreation);
        meterRegistry.remove(totalConnectionGauge);
        meterRegistry.remove(idleConnectionGauge);
        meterRegistry.remove(activeConnectionGauge);
        meterRegistry.remove(pendingConnectionGauge);
        meterRegistry.remove(maxConnectionGauge);
        meterRegistry.remove(minConnectionGauge);
    }
}
