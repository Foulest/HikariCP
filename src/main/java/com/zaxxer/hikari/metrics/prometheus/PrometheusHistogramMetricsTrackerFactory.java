package com.zaxxer.hikari.metrics.prometheus;

import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory.RegistrationStatus;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory.RegistrationStatus.REGISTERED;

/**
 * <pre>{@code
 * HikariConfig config = new HikariConfig();
 * config.setMetricsTrackerFactory(new PrometheusHistogramMetricsTrackerFactory());
 * }</pre>
 */
@SuppressWarnings("unused")
public class PrometheusHistogramMetricsTrackerFactory implements MetricsTrackerFactory {

    private final static Map<CollectorRegistry, RegistrationStatus> registrationStatuses = new ConcurrentHashMap<>();

    private final HikariCPCollector collector = new HikariCPCollector();
    private final CollectorRegistry collectorRegistry;

    /**
     * Default Constructor. The Hikari metrics are registered to the default
     * collector registry ({@code CollectorRegistry.defaultRegistry}).
     */
    public PrometheusHistogramMetricsTrackerFactory() {
        this(CollectorRegistry.defaultRegistry);
    }

    /**
     * Constructor that allows to pass in a {@link CollectorRegistry} to which the
     * Hikari metrics are registered.
     */
    public PrometheusHistogramMetricsTrackerFactory(CollectorRegistry collectorRegistry) {
        this.collectorRegistry = collectorRegistry;
    }

    @Override
    public IMetricsTracker create(String poolName, PoolStats poolStats) {
        registerCollector(collector, collectorRegistry);
        collector.add(poolName, poolStats);
        return new PrometheusHistogramMetricsTracker(poolName, collectorRegistry, collector);
    }

    private void registerCollector(Collector collector, CollectorRegistry collectorRegistry) {
        if (registrationStatuses.putIfAbsent(collectorRegistry, REGISTERED) == null) {
            collector.register(collectorRegistry);
        }
    }
}
