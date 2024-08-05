package com.zaxxer.hikari.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * A resolution-independent provider of current time-stamps and elapsed time
 * calculations.
 *
 * @author Brett Wooldridge
 */
@SuppressWarnings("unused")
public interface ClockSource {

    ClockSource CLOCK = Factory.create();

    /**
     * Get the current time-stamp (resolution is opaque).
     *
     * @return the current time-stamp
     */
    static long currentTime() {
        return CLOCK.currentTime0();
    }

    long currentTime0();

    /**
     * Convert an opaque time-stamp returned by currentTime() into
     * milliseconds.
     *
     * @param time an opaque time-stamp returned by an instance of this class
     * @return the time-stamp in milliseconds
     */
    static long toMillis(long time) {
        return CLOCK.toMillis0(time);
    }

    long toMillis0(long time);

    /**
     * Convert an opaque time-stamp returned by currentTime() into
     * nanoseconds.
     *
     * @param time an opaque time-stamp returned by an instance of this class
     * @return the time-stamp in nanoseconds
     */
    static long toNanos(long time) {
        return CLOCK.toNanos0(time);
    }

    long toNanos0(long time);

    /**
     * Convert an opaque time-stamp returned by currentTime() into an
     * elapsed time in milliseconds, based on the current instant in time.
     *
     * @param startTime an opaque time-stamp returned by an instance of this class
     * @return the elapsed time between startTime and now in milliseconds
     */
    static long elapsedMillis(long startTime) {
        return CLOCK.elapsedMillis0(startTime);
    }

    long elapsedMillis0(long startTime);

    /**
     * Get the difference in milliseconds between two opaque time-stamps returned
     * by currentTime().
     *
     * @param startTime an opaque time-stamp returned by an instance of this class
     * @param endTime   an opaque time-stamp returned by an instance of this class
     * @return the elapsed time between startTime and endTime in milliseconds
     */
    static long elapsedMillis(long startTime, long endTime) {
        return CLOCK.elapsedMillis0(startTime, endTime);
    }

    long elapsedMillis0(long startTime, long endTime);

    /**
     * Convert an opaque time-stamp returned by currentTime() into an
     * elapsed time in milliseconds, based on the current instant in time.
     *
     * @param startTime an opaque time-stamp returned by an instance of this class
     * @return the elapsed time between startTime and now in milliseconds
     */
    static long elapsedNanos(long startTime) {
        return CLOCK.elapsedNanos0(startTime);
    }

    long elapsedNanos0(long startTime);

    /**
     * Get the difference in nanoseconds between two opaque time-stamps returned
     * by currentTime().
     *
     * @param startTime an opaque time-stamp returned by an instance of this class
     * @param endTime   an opaque time-stamp returned by an instance of this class
     * @return the elapsed time between startTime and endTime in nanoseconds
     */
    static long elapsedNanos(long startTime, long endTime) {
        return CLOCK.elapsedNanos0(startTime, endTime);
    }

    long elapsedNanos0(long startTime, long endTime);

    /**
     * Return the specified opaque time-stamp plus the specified number of milliseconds.
     *
     * @param time   an opaque time-stamp
     * @param millis milliseconds to add
     * @return a new opaque time-stamp
     */
    static long plusMillis(long time, long millis) {
        return CLOCK.plusMillis0(time, millis);
    }

    long plusMillis0(long time, long millis);

    /**
     * Get the TimeUnit the ClockSource is denominated in.
     *
     * @return the TimeUnit
     */
    static TimeUnit getSourceTimeUnit() {
        return CLOCK.getSourceTimeUnit0();
    }

    TimeUnit getSourceTimeUnit0();

    /**
     * Get a String representation of the elapsed time in appropriate magnitude terminology.
     *
     * @param startTime an opaque time-stamp
     * @param endTime   an opaque time-stamp
     * @return a string representation of the elapsed time interval
     */
    static String elapsedDisplayString(long startTime, long endTime) {
        return CLOCK.elapsedDisplayString0(startTime, endTime);
    }

    default String elapsedDisplayString0(long startTime, long endTime) {
        long elapsedNanos = elapsedNanos0(startTime, endTime);
        StringBuilder sb = new StringBuilder(elapsedNanos < 0 ? "-" : "");
        elapsedNanos = Math.abs(elapsedNanos);

        for (TimeUnit unit : TimeUnitConstants.TIMEUNITS_DESCENDING) {
            long converted = unit.convert(elapsedNanos, TimeUnit.NANOSECONDS);

            if (converted > 0) {
                sb.append(converted).append(TimeUnitConstants.TIMEUNIT_DISPLAY_VALUES[unit.ordinal()]);
                elapsedNanos -= TimeUnit.NANOSECONDS.convert(converted, unit);
            }
        }
        return sb.toString();
    }

    /**
     * Factory class used to create a platform-specific ClockSource.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    final class Factory {

        @Contract(" -> new")
        private static @NotNull ClockSource create() {
            String os = System.getProperty("os.name");

            if ("Mac OS X".equals(os)) {
                return new MillisecondClockSource();
            }
            return new NanosecondClockSource();
        }
    }

    @NoArgsConstructor
    final class MillisecondClockSource implements ClockSource {

        @Override
        public long currentTime0() {
            return System.currentTimeMillis();
        }

        @Override
        public long elapsedMillis0(long startTime) {
            return System.currentTimeMillis() - startTime;
        }

        @Override
        public long elapsedMillis0(long startTime, long endTime) {
            return endTime - startTime;
        }

        @Override
        public long elapsedNanos0(long startTime) {
            return TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - startTime);
        }

        @Override
        public long elapsedNanos0(long startTime, long endTime) {
            return TimeUnit.MILLISECONDS.toNanos(endTime - startTime);
        }

        @Override
        public long toMillis0(long time) {
            return time;
        }

        @Override
        public long toNanos0(long time) {
            return TimeUnit.MILLISECONDS.toNanos(time);
        }

        @Override
        public long plusMillis0(long time, long millis) {
            return time + millis;
        }

        @Override
        public TimeUnit getSourceTimeUnit0() {
            return TimeUnit.MILLISECONDS;
        }
    }

    @NoArgsConstructor
    class NanosecondClockSource implements ClockSource {

        @Override
        public long currentTime0() {
            return System.nanoTime();
        }

        @Override
        public long toMillis0(long time) {
            return TimeUnit.NANOSECONDS.toMillis(time);
        }

        @Override
        public long toNanos0(long time) {
            return time;
        }

        @Override
        public long elapsedMillis0(long startTime) {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        }

        @Override
        public long elapsedMillis0(long startTime, long endTime) {
            return TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        }

        @Override
        public long elapsedNanos0(long startTime) {
            return System.nanoTime() - startTime;
        }

        @Override
        public long elapsedNanos0(long startTime, long endTime) {
            return endTime - startTime;
        }

        @Override
        public long plusMillis0(long time, long millis) {
            return time + TimeUnit.MILLISECONDS.toNanos(millis);
        }

        @Override
        public TimeUnit getSourceTimeUnit0() {
            return TimeUnit.NANOSECONDS;
        }
    }
}
