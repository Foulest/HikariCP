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
package com.zaxxer.hikari.metrics.dropwizard;

import com.codahale.metrics.*;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.PoolStats;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

@Getter
public final class CodaHaleMetricsTracker implements IMetricsTracker {

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

    private final String poolName;
    private final Timer connectionAcquisitionTimer;
    private final Histogram connectionDurationHistogram;
    private final Histogram connectionCreationHistogram;
    private final Meter connectionTimeoutMeter;
    private final MetricRegistry registry;

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

    @Override
    public void close() {
        removeFromRegistry(METRIC_NAME_WAIT, METRIC_NAME_USAGE, METRIC_NAME_CONNECT,
                METRIC_NAME_TIMEOUT_RATE, METRIC_NAME_TOTAL_CONNECTIONS);

        removeFromRegistry(METRIC_NAME_IDLE_CONNECTIONS, METRIC_NAME_ACTIVE_CONNECTIONS,
                METRIC_NAME_PENDING_CONNECTIONS, METRIC_NAME_MAX_CONNECTIONS, METRIC_NAME_MIN_CONNECTIONS);
    }

    private void removeFromRegistry(String metricNameWait, String metricNameUsage, String metricNameConnect,
                                    String metricNameTimeoutRate, String metricNameTotalConnections) {
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, metricNameWait));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, metricNameUsage));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, metricNameConnect));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, metricNameTimeoutRate));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, metricNameTotalConnections));
    }

    @Override
    public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
        connectionAcquisitionTimer.update(elapsedAcquiredNanos, TimeUnit.NANOSECONDS);
    }

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
