package com.zaxxer.hikari;

import java.sql.SQLException;

/**
 * Users can implement this interface to override the default SQLException handling
 * of HikariCP.  By the time an instance of this interface is invoked HikariCP has
 * already made a determination to evict the Connection from the pool.
 * <p>
 * If the {@link #adjudicate(SQLException)} method returns {@link Override#CONTINUE_EVICT} the eviction will occur,
 * but if the method returns {@link Override#DO_NOT_EVICT} the eviction will be elided.
 */
public interface SQLExceptionOverride {

    enum Override {
        CONTINUE_EVICT,
        DO_NOT_EVICT
    }

    /**
     * If this method returns {@link Override#CONTINUE_EVICT} then Connection eviction will occur, but if it
     * returns {@link Override#DO_NOT_EVICT} the eviction will be elided.
     *
     * @param ignored the #SQLException to adjudicate
     * @return either one of {@link Override#CONTINUE_EVICT} or {@link Override#DO_NOT_EVICT}
     */
    @SuppressWarnings("SameReturnValue")
    default Override adjudicate(SQLException ignored) {
        return Override.CONTINUE_EVICT;
    }
}
