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

    default boolean adjudicateAnyway() {
        return false;
    }
}
