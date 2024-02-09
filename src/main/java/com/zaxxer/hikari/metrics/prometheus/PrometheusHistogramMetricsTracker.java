package com.zaxxer.hikari.metrics.prometheus;

import com.zaxxer.hikari.metrics.IMetricsTracker;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory.RegistrationStatus;
import static com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory.RegistrationStatus.REGISTERED;

/**
 * Alternative Prometheus metrics tracker using a Histogram instead of Summary
 * <p>
 * This is an alternative metrics tracker that doesn't use a {@link io.prometheus.client.Summary}.
 * Summaries require heavy locks that might cause performance issues.
 * Source: <a href="https://github.com/prometheus/client_java/issues/328">GitHub Issues Link</a>
 *
 * @see PrometheusMetricsTracker
 */
class PrometheusHistogramMetricsTracker implements IMetricsTracker {

    private static final Counter CONNECTION_TIMEOUT_COUNTER = Counter.build()
            .name("hikaricp_connection_timeout_total")
            .labelNames("pool")
            .help("Connection timeout total count")
            .create();

    private static final Histogram ELAPSED_ACQUIRED_HISTOGRAM =
            registerHistogram("hikaricp_connection_acquired_nanos",
                    "Connection acquired time (ns)", 1_000);

    private static final Histogram ELAPSED_BORROWED_HISTOGRAM =
            registerHistogram("hikaricp_connection_usage_millis",
                    "Connection usage (ms)", 1);

    private static final Histogram ELAPSED_CREATION_HISTOGRAM =
            registerHistogram("hikaricp_connection_creation_millis",
                    "Connection creation (ms)", 1);

    private final Counter.Child connectionTimeoutCounterChild;

    private static Histogram registerHistogram(String name, String help, double bucketStart) {
        return Histogram.build()
                .name(name)
                .labelNames("pool")
                .help(help)
                .exponentialBuckets(bucketStart, 2.0, 11)
                .create();
    }

    private final static Map<CollectorRegistry, RegistrationStatus> registrationStatuses = new ConcurrentHashMap<>();

    private final String poolName;
    private final HikariCPCollector hikariCPCollector;

    private final Histogram.Child elapsedAcquiredHistogramChild;
    private final Histogram.Child elapsedBorrowedHistogramChild;
    private final Histogram.Child elapsedCreationHistogramChild;

    PrometheusHistogramMetricsTracker(String poolName, CollectorRegistry collectorRegistry,
                                      HikariCPCollector hikariCPCollector) {
        registerMetrics(collectorRegistry);
        this.poolName = poolName;
        this.hikariCPCollector = hikariCPCollector;
        connectionTimeoutCounterChild = CONNECTION_TIMEOUT_COUNTER.labels(poolName);
        elapsedAcquiredHistogramChild = ELAPSED_ACQUIRED_HISTOGRAM.labels(poolName);
        elapsedBorrowedHistogramChild = ELAPSED_BORROWED_HISTOGRAM.labels(poolName);
        elapsedCreationHistogramChild = ELAPSED_CREATION_HISTOGRAM.labels(poolName);
    }

    private void registerMetrics(CollectorRegistry collectorRegistry) {
        if (registrationStatuses.putIfAbsent(collectorRegistry, REGISTERED) == null) {
            CONNECTION_TIMEOUT_COUNTER.register(collectorRegistry);
            ELAPSED_ACQUIRED_HISTOGRAM.register(collectorRegistry);
            ELAPSED_BORROWED_HISTOGRAM.register(collectorRegistry);
            ELAPSED_CREATION_HISTOGRAM.register(collectorRegistry);
        }
    }

    @Override
    public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
        elapsedAcquiredHistogramChild.observe(elapsedAcquiredNanos);
    }

    @Override
    public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
        elapsedBorrowedHistogramChild.observe(elapsedBorrowedMillis);
    }

    @Override
    public void recordConnectionCreatedMillis(long connectionCreatedMillis) {
        elapsedCreationHistogramChild.observe(connectionCreatedMillis);
    }

    @Override
    public void recordConnectionTimeout() {
        connectionTimeoutCounterChild.inc();
    }

    @Override
    public void close() {
        hikariCPCollector.remove(poolName);
        CONNECTION_TIMEOUT_COUNTER.remove(poolName);
        ELAPSED_ACQUIRED_HISTOGRAM.remove(poolName);
        ELAPSED_BORROWED_HISTOGRAM.remove(poolName);
        ELAPSED_CREATION_HISTOGRAM.remove(poolName);
    }
}
