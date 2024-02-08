package com.zaxxer.hikari.metrics;

/**
 * @author Brett Wooldridge
 */
public interface IMetricsTracker extends AutoCloseable {

    default void recordConnectionCreatedMillis(long connectionCreatedMillis) {
    }

    default void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
    }

    default void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
    }

    default void recordConnectionTimeout() {
    }

    @Override
    default void close() {
    }
}
