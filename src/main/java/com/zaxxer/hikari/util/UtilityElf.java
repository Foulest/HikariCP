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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.concurrent.*;

/**
 * @author Brett Wooldridge
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class UtilityElf {

    /**
     * @return null if string is null or empty
     */
    public static @Nullable String getNullIfEmpty(String text) {
        if (text == null) {
            return null;
        }

        String trimmedText = text.trim();
        return trimmedText.isEmpty() ? null : trimmedText;
    }

    /**
     * Sleep and suppress InterruptedException (but re-signal it).
     *
     * @param millis the number of milliseconds to sleep
     */
    public static void quietlySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Checks whether an object is an instance of given type without throwing exception when the class is not loaded.
     *
     * @param obj       the object to check
     * @param className String class
     * @return true if object is assignable from the type, false otherwise or when the class cannot be loaded
     */
    public static boolean safeIsAssignableFrom(@NotNull Object obj, String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return clazz.isAssignableFrom(obj.getClass());
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    /**
     * Create and instance of the specified class using the constructor matching the specified
     * arguments.
     *
     * @param <T>       the class type
     * @param className the name of the class to instantiate
     * @param clazz     a class to cast the result as
     * @param args      arguments to a constructor
     * @return an instance of the specified class
     */
    public static <T> @Nullable T createInstance(String className, Class<T> clazz, Object... args) {
        if (className == null) {
            return null;
        }

        try {
            Class<?> loaded = UtilityElf.class.getClassLoader().loadClass(className);

            if (args.length == 0) {
                return clazz.cast(loaded.getDeclaredConstructor().newInstance());
            }

            Class<?>[] argClasses = new Class<?>[args.length];
            int size = args.length;

            for (int i = 0; i < size; i++) {
                argClasses[i] = args[i].getClass();
            }

            Constructor<?> constructor = loaded.getConstructor(argClasses);
            return clazz.cast(constructor.newInstance(args));
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                 | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create a ThreadPoolExecutor.
     *
     * @param queueSize     the queue size
     * @param threadName    the thread name
     * @param threadFactory an optional ThreadFactory
     * @param policy        the RejectedExecutionHandler policy
     * @return a ThreadPoolExecutor
     */
    public static @NotNull ThreadPoolExecutor createThreadPoolExecutor(int queueSize, String threadName,
                                                                       ThreadFactory threadFactory,
                                                                       RejectedExecutionHandler policy) {
        if (threadFactory == null) {
            threadFactory = new DefaultThreadFactory(threadName, true);
        }

        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 5, TimeUnit.SECONDS, queue, threadFactory, policy
        );

        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Create a ThreadPoolExecutor.
     *
     * @param queue         the BlockingQueue to use
     * @param threadName    the thread name
     * @param threadFactory an optional ThreadFactory
     * @param policy        the RejectedExecutionHandler policy
     * @return a ThreadPoolExecutor
     */
    public static @NotNull ThreadPoolExecutor createThreadPoolExecutor(BlockingQueue<Runnable> queue,
                                                                       String threadName, ThreadFactory threadFactory,
                                                                       RejectedExecutionHandler policy) {
        if (threadFactory == null) {
            threadFactory = new DefaultThreadFactory(threadName, true);
        }

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 5, TimeUnit.SECONDS, queue, threadFactory, policy
        );

        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    // ***********************************************************************
    //                       Misc. public methods
    // ***********************************************************************

    /**
     * Get the int value of a transaction isolation level by name.
     *
     * @param transactionIsolationName the name of the transaction isolation level
     * @return the int value of the isolation level or -1
     */
    public static int getTransactionIsolation(String transactionIsolationName) {
        if (transactionIsolationName != null) {
            try {
                // use the english locale to avoid the infamous turkish locale bug
                String upperCaseIsolationLevelName = transactionIsolationName.toUpperCase(Locale.ROOT);
                return IsolationLevel.valueOf(upperCaseIsolationLevelName).getLevelId();
            } catch (IllegalArgumentException ex) {
                // legacy support for passing an integer version of the isolation level
                try {
                    int level = Integer.parseInt(transactionIsolationName);

                    for (IsolationLevel iso : IsolationLevel.values()) {
                        if (iso.getLevelId() == level) {
                            return iso.getLevelId();
                        }
                    }
                    throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName, nfe);
                }
            }
        }
        return -1;
    }

    @AllArgsConstructor
    public static final class DefaultThreadFactory implements ThreadFactory {

        private final String threadName;
        private final boolean daemon;

        @Override
        public @NotNull Thread newThread(@NotNull Runnable runnable) {
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(daemon);
            return thread;
        }
    }
}
