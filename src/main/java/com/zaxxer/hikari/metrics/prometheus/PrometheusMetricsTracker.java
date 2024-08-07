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
package com.zaxxer.hikari.metrics.prometheus;

import com.zaxxer.hikari.metrics.IMetricsTracker;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class PrometheusMetricsTracker implements IMetricsTracker {

    private static final Counter CONNECTION_TIMEOUT_COUNTER = Counter.build()
            .name("hikaricp_connection_timeout_total")
            .labelNames("pool")
            .help("Connection timeout total count")
            .create();

    private static final Summary ELAPSED_ACQUIRED_SUMMARY =
            createSummary("hikaricp_connection_acquired_nanos",
                    "Connection acquired time (ns)");

    private static final Summary ELAPSED_USAGE_SUMMARY =
            createSummary("hikaricp_connection_usage_millis",
                    "Connection usage (ms)");

    private static final Summary ELAPSED_CREATION_SUMMARY =
            createSummary("hikaricp_connection_creation_millis",
                    "Connection creation (ms)");

    private static final Map<CollectorRegistry, PrometheusMetricsTrackerFactory.RegistrationStatus> registrationStatuses = new ConcurrentHashMap<>();

    private final String poolName;
    private final HikariCPCollector hikariCPCollector;
    private final Counter.Child connectionTimeoutCounterChild;
    private final Summary.Child elapsedAcquiredSummaryChild;
    private final Summary.Child elapsedUsageSummaryChild;
    private final Summary.Child elapsedCreationSummaryChild;

    PrometheusMetricsTracker(String poolName, CollectorRegistry collectorRegistry, HikariCPCollector hikariCPCollector) {
        registerMetrics(collectorRegistry);
        this.poolName = poolName;
        this.hikariCPCollector = hikariCPCollector;
        connectionTimeoutCounterChild = CONNECTION_TIMEOUT_COUNTER.labels(poolName);
        elapsedAcquiredSummaryChild = ELAPSED_ACQUIRED_SUMMARY.labels(poolName);
        elapsedUsageSummaryChild = ELAPSED_USAGE_SUMMARY.labels(poolName);
        elapsedCreationSummaryChild = ELAPSED_CREATION_SUMMARY.labels(poolName);
    }

    private static void registerMetrics(CollectorRegistry collectorRegistry) {
        if (registrationStatuses.putIfAbsent(collectorRegistry, PrometheusMetricsTrackerFactory.RegistrationStatus.REGISTERED) == null) {
            CONNECTION_TIMEOUT_COUNTER.register(collectorRegistry);
            ELAPSED_ACQUIRED_SUMMARY.register(collectorRegistry);
            ELAPSED_USAGE_SUMMARY.register(collectorRegistry);
            ELAPSED_CREATION_SUMMARY.register(collectorRegistry);
        }
    }

    @Override
    public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
        elapsedAcquiredSummaryChild.observe(elapsedAcquiredNanos);
    }

    @Override
    public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
        elapsedUsageSummaryChild.observe(elapsedBorrowedMillis);
    }

    @Override
    public void recordConnectionCreatedMillis(long connectionCreatedMillis) {
        elapsedCreationSummaryChild.observe(connectionCreatedMillis);
    }

    @Override
    public void recordConnectionTimeout() {
        connectionTimeoutCounterChild.inc();
    }

    private static Summary createSummary(String name, String help) {
        return Summary.build()
                .name(name)
                .labelNames("pool")
                .help(help)
                .quantile(0.5, 0.05)
                .quantile(0.95, 0.01)
                .quantile(0.99, 0.001)
                .maxAgeSeconds(TimeUnit.MINUTES.toSeconds(5))
                .ageBuckets(5)
                .create();
    }

    @Override
    public void close() {
        hikariCPCollector.remove(poolName);
        CONNECTION_TIMEOUT_COUNTER.remove(poolName);
        ELAPSED_ACQUIRED_SUMMARY.remove(poolName);
        ELAPSED_USAGE_SUMMARY.remove(poolName);
        ELAPSED_CREATION_SUMMARY.remove(poolName);
    }
}
