package com.zaxxer.hikari.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.concurrent.TimeUnit;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class TimeUnitConstants {

    static final TimeUnit[] TIMEUNITS_DESCENDING = {TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS};
    static final String[] TIMEUNIT_DISPLAY_VALUES = {"d", "h", "m", "s", "ms", "µs", "ns"};
}
