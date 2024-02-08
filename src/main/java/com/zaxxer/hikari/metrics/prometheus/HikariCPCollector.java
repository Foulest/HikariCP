package com.zaxxer.hikari.metrics.prometheus;

import com.zaxxer.hikari.metrics.PoolStats;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class HikariCPCollector extends Collector {

    private static final List<String> LABEL_NAMES = Collections.singletonList("pool");

    private final Map<String, PoolStats> poolStatsMap = new ConcurrentHashMap<>();

    @Override
    public List<MetricFamilySamples> collect() {
        return Arrays.asList(
                createGauge("hikaricp_active_connections", "Active connections", PoolStats::getActiveConnections),
                createGauge("hikaricp_idle_connections", "Idle connections", PoolStats::getIdleConnections),
                createGauge("hikaricp_pending_threads", "Pending threads", PoolStats::getPendingThreads),
                createGauge("hikaricp_connections", "The number of current connections", PoolStats::getTotalConnections),
                createGauge("hikaricp_max_connections", "Max connections", PoolStats::getMaxConnections),
                createGauge("hikaricp_min_connections", "Min connections", PoolStats::getMinConnections)
        );
    }

    void add(String name, PoolStats poolStats) {
        poolStatsMap.put(name, poolStats);
    }

    void remove(String name) {
        poolStatsMap.remove(name);
    }

    private @NotNull GaugeMetricFamily createGauge(String metric, String help,
                                                   Function<PoolStats, Integer> metricValueFunction) {
        GaugeMetricFamily metricFamily = new GaugeMetricFamily(metric, help, LABEL_NAMES);

        poolStatsMap.forEach((k, v) -> metricFamily.addMetric(
                Collections.singletonList(k),
                metricValueFunction.apply(v)
        ));
        return metricFamily;
    }
}
