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
