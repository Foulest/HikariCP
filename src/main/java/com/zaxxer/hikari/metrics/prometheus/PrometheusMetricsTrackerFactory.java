package com.zaxxer.hikari.metrics.prometheus;

import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory.RegistrationStatus.REGISTERED;

/**
 * <pre>{@code
 * HikariConfig config = new HikariConfig();
 * config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
 * }</pre>
 * or
 * <pre>{@code
 * config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory(new CollectorRegistry()));
 * }</pre>
 * <p>
 * Note: the internal {@see io.prometheus.client.Summary} requires heavy locks. Consider using
 * {@see PrometheusHistogramMetricsTrackerFactory} if performance plays a role and you don't need the summary per se.
 */
public class PrometheusMetricsTrackerFactory implements MetricsTrackerFactory {

    private final static Map<CollectorRegistry, RegistrationStatus> registrationStatuses = new ConcurrentHashMap<>();

    private final HikariCPCollector collector = new HikariCPCollector();

    private final CollectorRegistry collectorRegistry;

    enum RegistrationStatus {
        REGISTERED
    }

    /**
     * Default Constructor. The Hikari metrics are registered to the default
     * collector registry ({@code CollectorRegistry.defaultRegistry}).
     */
    public PrometheusMetricsTrackerFactory() {
        this(CollectorRegistry.defaultRegistry);
    }

    /**
     * Constructor that allows to pass in a {@link CollectorRegistry} to which the
     * Hikari metrics are registered.
     */
    public PrometheusMetricsTrackerFactory(CollectorRegistry collectorRegistry) {
        this.collectorRegistry = collectorRegistry;
    }

    @Override
    public IMetricsTracker create(String poolName, PoolStats poolStats) {
        registerCollector(collector, collectorRegistry);
        collector.add(poolName, poolStats);
        return new PrometheusMetricsTracker(poolName, collectorRegistry, collector);
    }

    private void registerCollector(Collector collector, CollectorRegistry collectorRegistry) {
        if (registrationStatuses.putIfAbsent(collectorRegistry, REGISTERED) == null) {
            collector.register(collectorRegistry);
        }
    }
}
