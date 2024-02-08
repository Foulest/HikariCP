package com.zaxxer.hikari.metrics.dropwizard;

import com.codahale.metrics.MetricRegistry;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

@Getter
@AllArgsConstructor
public final class CodahaleMetricsTrackerFactory implements MetricsTrackerFactory {

    private final MetricRegistry registry;

    @Override
    @Contract("_, _ -> new")
    public @NotNull IMetricsTracker create(String poolName, PoolStats poolStats) {
        return new CodaHaleMetricsTracker(poolName, poolStats, registry);
    }
}
