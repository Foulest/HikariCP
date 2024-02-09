package com.zaxxer.hikari.util;

import com.zaxxer.hikari.HikariConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that reflectively sets bean properties on a target object.
 *
 * @author Brett Wooldridge
 */
public final class PropertyElf {

    private static final Pattern GETTER_PATTERN = Pattern.compile("(get|is)[A-Z].+");

    private PropertyElf() {
        // cannot be constructed
    }

    public static void setTargetFromProperties(Object target, Properties properties) {
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
        HashSet<String> set = new HashSet<>();
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

    public static @Nullable Object getProperty(String propName, Object target) {
        try {
            // use the english locale to avoid the infamous turkish locale bug
            String capitalized = "get" + propName.substring(0, 1).toUpperCase(Locale.ENGLISH)
                    + propName.substring(1);
            Method method = target.getClass().getMethod(capitalized);
            return method.invoke(target);
        } catch (Exception ex) {
            try {
                String capitalized = "is" + propName.substring(0, 1).toUpperCase(Locale.ENGLISH)
                        + propName.substring(1);
                Method method = target.getClass().getMethod(capitalized);
                return method.invoke(target);
            } catch (Exception e2) {
                return null;
            }
        }
    }

    public static @NotNull Properties copyProperties(@NotNull Properties props) {
        Properties copy = new Properties();
        props.forEach((key, value) -> copy.setProperty(key.toString(), value.toString()));
        return copy;
    }

    private static void setProperty(Object target, @NotNull String propName,
                                    Object propValue, @NotNull List<Method> methods) {
        Logger logger = LoggerFactory.getLogger(PropertyElf.class);

        // use the english locale to avoid the infamous turkish locale bug
        // https://www.moserware.com/2008/02/does-your-code-pass-turkey-test.html
        String methodName = "set" + propName.substring(0, 1).toUpperCase(Locale.ENGLISH)
                + propName.substring(1);

        Method writeMethod = methods.stream().filter(m -> m.getName().equals(methodName)
                && m.getParameterCount() == 1).findFirst().orElse(null);

        if (writeMethod == null) {
            String methodName2 = "set" + propName.toUpperCase(Locale.ENGLISH);
            writeMethod = methods.stream().filter(m -> m.getName().equals(methodName2)
                    && m.getParameterCount() == 1).findFirst().orElse(null);
        }

        if (writeMethod == null) {
            logger.error("Property {} does not exist on target {}", propName, target.getClass());
            throw new RuntimeException(String.format("Property %s does not exist on target %s",
                    propName, target.getClass()));
        }

        try {
            Class<?> paramClass = writeMethod.getParameterTypes()[0];

            if (paramClass == int.class) {
                writeMethod.invoke(target, Integer.parseInt(propValue.toString()));
            } else if (paramClass == long.class) {
                writeMethod.invoke(target, Long.parseLong(propValue.toString()));
            } else if (paramClass == short.class) {
                writeMethod.invoke(target, Short.parseShort(propValue.toString()));
            } else if (paramClass == boolean.class || paramClass == Boolean.class) {
                writeMethod.invoke(target, Boolean.parseBoolean(propValue.toString()));
            } else if (paramClass == String.class) {
                writeMethod.invoke(target, propValue.toString());
            } else {
                try {
                    logger.debug("Try to create a new instance of \"{}\"", propValue);
                    writeMethod.invoke(target, Class.forName(propValue.toString())
                            .getDeclaredConstructor().newInstance());
                } catch (InstantiationException | ClassNotFoundException ex) {
                    logger.debug("Class \"{}\" not found or could not instantiate it (Default constructor)", propValue);
                    writeMethod.invoke(target, propValue);
                }
            }
        } catch (Exception ex) {
            logger.error("Failed to set property {} on target {}", propName, target.getClass(), ex);
            throw new RuntimeException(ex);
        }
    }
}
