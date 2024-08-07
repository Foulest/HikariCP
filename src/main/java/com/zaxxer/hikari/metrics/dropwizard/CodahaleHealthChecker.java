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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import lombok.AllArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Provides Dropwizard HealthChecks.  Two health checks are provided:
 * <ul>
 *   <li>ConnectivityCheck</li>
 *   <li>Connection99Percent</li>
 * </ul>
 * The ConnectivityCheck will use the {@code connectionTimeout}, unless the health check property
 * {@code connectivityCheckTimeoutMs} is defined. However, if either the {@code connectionTimeout}
 * or the {@code connectivityCheckTimeoutMs} is 0 (infinite), a timeout of 10 seconds will be used.
 * <p>
 * The Connection99Percent health check will only be registered if the health check property
 * {@code expected99thPercentileMs} is defined and greater than 0.
 *
 * @author Brett Wooldridge
 */
public final class CodahaleHealthChecker {

    /**
     * Register Dropwizard health checks.
     *
     * @param pool         the pool to register health checks for
     * @param hikariConfig the pool configuration
     * @param registry     the HealthCheckRegistry into which checks will be registered
     */
    public static void registerHealthChecks(HikariPool pool,
                                            @NotNull HikariConfig hikariConfig,
                                            @NotNull HealthCheckRegistry registry) {
        Map<Object, Object> healthCheckProperties = hikariConfig.getHealthCheckProperties();
        MetricRegistry metricRegistry = (MetricRegistry) hikariConfig.getMetricRegistry();

        long checkTimeoutMs = Long.parseLong((String) healthCheckProperties.getOrDefault("connectivityCheckTimeoutMs",
                String.valueOf(hikariConfig.getConnectionTimeout())));

        registry.register(MetricRegistry.name(hikariConfig.getPoolName(), "pool", "ConnectivityCheck"),
                new ConnectivityHealthCheck(pool, checkTimeoutMs));

        long expected99thPercentile = Long.parseLong((String) healthCheckProperties.getOrDefault(
                "expected99thPercentileMs", "0"));

        if (metricRegistry != null && expected99thPercentile > 0) {
            SortedMap<String, Timer> timers = metricRegistry.getTimers((name, metric) ->
                    name.equals(MetricRegistry.name(hikariConfig.getPoolName(), "pool", "Wait")));

            if (!timers.isEmpty()) {
                Timer timer = timers.entrySet().iterator().next().getValue();

                registry.register(MetricRegistry.name(hikariConfig.getPoolName(), "pool",
                        "Connection99Percent"), new Connection99Percent(timer, expected99thPercentile));
            }
        }
    }

    private CodahaleHealthChecker() {
        // private constructor
    }

    private static class ConnectivityHealthCheck extends HealthCheck {

        private final HikariPool pool;
        private final long checkTimeoutMs;

        ConnectivityHealthCheck(HikariPool pool, long checkTimeoutMs) {
            this.pool = pool;
            this.checkTimeoutMs = (checkTimeoutMs > 0 && checkTimeoutMs != Integer.MAX_VALUE
                    ? checkTimeoutMs : TimeUnit.SECONDS.toMillis(10));
        }

        @Override
        protected Result check() {
            try (Connection ignored = pool.getConnection(checkTimeoutMs)) {
                return Result.healthy();
            } catch (SQLException ex) {
                return Result.unhealthy(ex);
            }
        }
    }

    @AllArgsConstructor
    private static class Connection99Percent extends HealthCheck {

        private final Timer waitTimer;
        private final long expected99thPercentile;

        @Override
        protected Result check() {
            long the99thPercentile = TimeUnit.NANOSECONDS.toMillis(
                    Math.round(waitTimer.getSnapshot().get99thPercentile())
            );

            return the99thPercentile <= expected99thPercentile ? Result.healthy()
                    : Result.unhealthy("99th percentile connection wait time of"
                    + " %dms exceeds the threshold %dms", the99thPercentile, expected99thPercentile);
        }
    }
}
