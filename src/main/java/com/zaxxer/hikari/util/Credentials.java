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
package com.zaxxer.hikari.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * A simple class to hold connection credentials and is designed to be immutable.
 */
@Getter
@AllArgsConstructor
public final class Credentials {

    private final String username;
    private final String password;

    /**
     * Construct an immutable Credentials object with the supplied username and password.
     *
     * @param username the username
     * @param password the password
     * @return a new Credentials object
     */
    @Contract(value = "_, _ -> new", pure = true)
    public static @NotNull Credentials of(String username, String password) {
        return new Credentials(username, password);
    }
}
