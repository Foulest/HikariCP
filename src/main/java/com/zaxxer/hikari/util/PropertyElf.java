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

import com.zaxxer.hikari.HikariConfig;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that reflectively sets bean properties on a target object.
 *
 * @author Brett Wooldridge
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PropertyElf {

    private static final Pattern GETTER_PATTERN = Pattern.compile("(get|is)[A-Z].+");

    public static void setTargetFromProperties(Object target, Map<Object, Object> properties) {
        if (target == null || properties == null) {
            return;
        }

        List<Method> methods = Arrays.asList(target.getClass().getMethods());

        properties.forEach((key, value) -> {
            if (target instanceof HikariConfig && key.toString().startsWith("dataSource.")) {
                ((HikariConfig) target).addDataSourceProperty(key.toString().substring("dataSource.".length()), value);
            } else {
                setProperty(target, key.toString(), value, methods);
            }
        });
    }

    /**
     * Get the bean-style property names for the specified object.
     *
     * @param targetClass the target object
     * @return a set of property names
     */
    public static @NotNull Set<String> getPropertyNames(@NotNull Class<?> targetClass) {
        Set<String> set = new HashSet<>();
        Matcher matcher = GETTER_PATTERN.matcher("");

        for (Method method : targetClass.getMethods()) {
            String name = method.getName();

            if (method.getParameterTypes().length == 0 && matcher.reset(name).matches()) {
                name = name.replaceFirst("(get|is)", "");
                name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
                set.add(name);
            }
        }
        return set;
    }

    public static @Nullable Object getProperty(@NotNull String propName, @NotNull Object target) {
        try {
            String capitalized = "get" + propName.substring(0, 1).toUpperCase(Locale.ROOT)
                    + propName.substring(1);
            Method method = target.getClass().getMethod(capitalized);
            return method.invoke(target);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                 | IllegalArgumentException | InvocationTargetException ex) {
            try {
                String capitalized = "is" + propName.substring(0, 1).toUpperCase(Locale.ROOT)
                        + propName.substring(1);
                Method method = target.getClass().getMethod(capitalized);
                return method.invoke(target);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                     | IllegalArgumentException | InvocationTargetException ignored) {
                return null;
            }
        }
    }

    public static @NotNull Properties copyProperties(@NotNull Map<Object, Object> properties) {
        Properties copy = new Properties();
        properties.forEach((key, value) -> copy.setProperty(key.toString(), value.toString()));
        return copy;
    }

    private static void setProperty(Object target, @NotNull String propName,
                                    Object propValue, @NotNull Collection<Method> methods) {
        String methodName = "set" + propName.substring(0, 1).toUpperCase(Locale.ROOT) + propName.substring(1);

        Method writeMethod = methods.stream().filter(m -> m.getName().equals(methodName)
                && m.getParameterCount() == 1).findFirst().orElse(null);

        if (writeMethod == null) {
            String methodName2 = "set" + propName.toUpperCase(Locale.ROOT);
            writeMethod = methods.stream().filter(m -> m.getName().equals(methodName2)
                    && m.getParameterCount() == 1).findFirst().orElse(null);
        }

        if (writeMethod == null) {
            log.error("Property {} does not exist on target {}", propName, target.getClass());
            throw new RuntimeException(String.format("Property %s does not exist on target %s", propName, target.getClass()));
        }

        try {
            Class<?> paramClass = writeMethod.getParameterTypes()[0];
            invokeWriteMethod(writeMethod, target, propValue, paramClass);
        } catch (IllegalAccessException | InvocationTargetException ex) {
            log.error("Failed to set property {} on target {}", propName, target.getClass(), ex);
            throw new RuntimeException(ex);
        }
    }

    private static void invokeWriteMethod(Method writeMethod, Object target, Object propValue, Class<?> paramClass)
            throws InvocationTargetException, IllegalAccessException {
        if (paramClass == int.class) {
            writeMethod.invoke(target, Integer.parseInt(propValue.toString()));
        } else if (paramClass == long.class) {
            writeMethod.invoke(target, Long.parseLong(propValue.toString()));
        } else if (paramClass == short.class) {
            writeMethod.invoke(target, Short.parseShort(propValue.toString()));
        } else if (paramClass == double.class) {
            writeMethod.invoke(target, Double.parseDouble(propValue.toString()));
        } else if (paramClass == boolean.class || paramClass == Boolean.class) {
            writeMethod.invoke(target, Boolean.parseBoolean(propValue.toString()));
        } else if (paramClass == String.class) {
            writeMethod.invoke(target, propValue.toString());
        } else {
            try {
                log.debug("Try to create a new instance of \"{}\"", propValue);
                writeMethod.invoke(target, Class.forName(propValue.toString()).getDeclaredConstructor().newInstance());
            } catch (InstantiationException | ClassNotFoundException ex) {
                log.debug("Class \"{}\" not found or could not instantiate it (Default constructor)", propValue);
                writeMethod.invoke(target, propValue);
            } catch (NoSuchMethodException ex) {
                ex.printStackTrace();
            }
        }
    }
}
