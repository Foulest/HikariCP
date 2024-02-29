package com.zaxxer.hikari;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.util.PropertyElf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.sql.Connection;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.zaxxer.hikari.util.UtilityElf.getNullIfEmpty;
import static com.zaxxer.hikari.util.UtilityElf.safeIsAssignableFrom;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Getter
@Setter
@SuppressWarnings({"SameParameterValue", "unused"})
public class HikariConfig implements HikariConfigMXBean {

    private static final char[] ID_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final long CONNECTION_TIMEOUT = SECONDS.toMillis(30);
    private static final long VALIDATION_TIMEOUT = SECONDS.toMillis(5);
    private static final long SOFT_TIMEOUT_FLOOR = Long.getLong("com.zaxxer.hikari.timeoutMs.floor", 250L);
    private static final long IDLE_TIMEOUT = MINUTES.toMillis(10);
    private static final long MAX_LIFETIME = MINUTES.toMillis(30);
    private static final long DEFAULT_KEEPALIVE_TIME = 0L;
    private static final long MIN_LIFETIME = SECONDS.toMillis(30);
    private static final long MIN_KEEPALIVE_TIME = MIN_LIFETIME; // Assuming the same minimum as lifetime for simplicity
    private static final long MIN_LEAK_DETECTION_THRESHOLD = SECONDS.toMillis(2);
    private static final int DEFAULT_POOL_SIZE = 10;
    private static final int MINIMUM_POOL_SIZE = 1;
    private static final String POOL_NAME_PREFIX = "HikariPool-";
    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(0);
    private static final boolean UNIT_TEST = false;

    // Properties changeable at runtime through the HikariConfigMXBean
    private volatile String catalog;
    private volatile String username;
    private volatile String password;
    private volatile long connectionTimeout;
    private volatile long validationTimeout;
    private volatile long idleTimeout;
    private volatile long leakDetectionThreshold;
    private volatile long maxLifetime;
    private volatile int maximumPoolSize;
    private volatile int minimumIdle;
    private long initializationFailTimeout;
    private String connectionInitSql;
    private String connectionTestQuery;
    private String dataSourceClassName;
    private String dataSourceJNDI;
    private String driverClassName;
    private String exceptionOverrideClassName;
    private String jdbcUrl;
    private String poolName;
    private String schema;
    private String transactionIsolation;
    private boolean autoCommit;
    private boolean readOnly;
    private boolean isolateInternalQueries;
    private boolean registerMbeans;
    private boolean allowPoolSuspension;
    private DataSource dataSource;
    private final Properties dataSourceProperties;
    private ThreadFactory threadFactory;
    private ScheduledExecutorService scheduledExecutor;
    private MetricsTrackerFactory metricsTrackerFactory;
    private Object metricRegistry;
    private Object healthCheckRegistry;
    private final Properties healthCheckProperties;
    private long keepaliveTime;

    private volatile boolean sealed;

    /**
     * Default constructor
     */
    public HikariConfig() {
        dataSourceProperties = new Properties();
        healthCheckProperties = new Properties();

        minimumIdle = -1;
        maximumPoolSize = -1;
        maxLifetime = MAX_LIFETIME;
        connectionTimeout = CONNECTION_TIMEOUT;
        validationTimeout = VALIDATION_TIMEOUT;
        idleTimeout = IDLE_TIMEOUT;
        initializationFailTimeout = 1;
        autoCommit = true;
        keepaliveTime = DEFAULT_KEEPALIVE_TIME;

        String systemProp = System.getProperty("hikaricp.configurationFile");
        if (systemProp != null) {
            loadProperties(systemProp);
        }
    }

    /**
     * Construct a HikariConfig from the specified properties object.
     *
     * @param properties the name of the property file
     */
    public HikariConfig(Properties properties) {
        this();
        PropertyElf.setTargetFromProperties(this, properties);
    }

    /**
     * Construct a HikariConfig from the specified property file name.  <code>propertyFileName</code>
     * will first be treated as a path in the file-system, and if that fails the
     * Class.getResourceAsStream(propertyFileName) will be tried.
     *
     * @param propertyFileName the name of the property file
     */
    public HikariConfig(String propertyFileName) {
        this();
        loadProperties(propertyFileName);
    }

    // ***********************************************************************
    //                       HikariConfigMXBean methods
    // ***********************************************************************

    @Override
    public void setConnectionTimeout(long connectionTimeoutMs) {
        if (connectionTimeoutMs == 0) {
            connectionTimeout = Integer.MAX_VALUE;
        } else if (connectionTimeoutMs < SOFT_TIMEOUT_FLOOR) {
            throw new IllegalArgumentException("connectionTimeout cannot be less than " + SOFT_TIMEOUT_FLOOR + "ms");
        } else {
            connectionTimeout = connectionTimeoutMs;
        }
    }

    @Override
    public void setIdleTimeout(long idleTimeoutMs) {
        if (idleTimeoutMs < 0) {
            throw new IllegalArgumentException("idleTimeout cannot be negative");
        }
        idleTimeout = idleTimeoutMs;
    }

    @Override
    public void setMaximumPoolSize(int maximumPoolSize) {
        if (maximumPoolSize < 1) {
            throw new IllegalArgumentException("maxPoolSize cannot be less than 1");
        }
        this.maximumPoolSize = maximumPoolSize;
    }

    @Override
    public void setMinimumIdle(int minimumIdle) {
        if (minimumIdle < 0) {
            throw new IllegalArgumentException("minimumIdle cannot be negative");
        }
        this.minimumIdle = minimumIdle;
    }

    @Override
    public void setValidationTimeout(long validationTimeout) {
        if (validationTimeout < SOFT_TIMEOUT_FLOOR) {
            throw new IllegalArgumentException("validationTimeout cannot be less than " + SOFT_TIMEOUT_FLOOR + "ms");
        }
        this.validationTimeout = validationTimeout;
    }

    // ***********************************************************************
    //                     All other configuration methods
    // ***********************************************************************

    /**
     * Set the SQL query to be executed to test the validity of connections. Using
     * the JDBC4 <code>Connection.isValid()</code> method to test connection validity can
     * be more efficient on some databases and is recommended.
     *
     * @param connectionTestQuery a SQL query string
     */
    public void setConnectionTestQuery(String connectionTestQuery) {
        checkIfSealed();
        this.connectionTestQuery = connectionTestQuery;
    }

    /**
     * Set the SQL string that will be executed on all new connections when they are
     * created, before they are added to the pool.  If this query fails, it will be
     * treated as a failed connection attempt.
     *
     * @param connectionInitSql the SQL to execute on new connections
     */
    public void setConnectionInitSql(String connectionInitSql) {
        checkIfSealed();
        this.connectionInitSql = connectionInitSql;
    }

    /**
     * Set a {@link DataSource} for the pool to explicitly wrap.  This setter is not
     * available through property file based initialization.
     *
     * @param dataSource a specific {@link DataSource} to be wrapped by the pool
     */
    public void setDataSource(DataSource dataSource) {
        checkIfSealed();
        this.dataSource = dataSource;
    }

    /**
     * Set the fully qualified class name of the JDBC {@link DataSource} that will be used create Connections.
     *
     * @param className the fully qualified name of the JDBC {@link DataSource} class
     */
    public void setDataSourceClassName(String className) {
        checkIfSealed();
        dataSourceClassName = className;
    }

    /**
     * Add a property (name/value pair) that will be used to configure the {@link DataSource}/{@link java.sql.Driver}.
     * <p>
     * In the case of a {@link DataSource}, the property names will be translated to Java setters following the Java Bean
     * naming convention.  For example, the property {@code cachePrepStmts} will translate into {@code setCachePrepStmts()}
     * with the {@code value} passed as a parameter.
     * <p>
     * In the case of a {@link java.sql.Driver}, the property will be added to a {@link Properties} instance that will
     * be passed to the driver during {@link java.sql.Driver#connect(String, Properties)} calls.
     *
     * @param propertyName the name of the property
     * @param value        the value to be used by the DataSource/Driver
     */
    public void addDataSourceProperty(String propertyName, Object value) {
        checkIfSealed();
        dataSourceProperties.put(propertyName, value);
    }

    public void setDataSourceJNDI(String jndiDataSource) {
        checkIfSealed();
        dataSourceJNDI = jndiDataSource;
    }

    public void setDataSourceProperties(Properties dsProperties) {
        checkIfSealed();
        dataSourceProperties.putAll(dsProperties);
    }

    public void setDriverClassName(String driverClassName) {
        checkIfSealed();
        Class<?> driverClass = attemptFromContextLoader(driverClassName);

        try {
            if (driverClass == null) {
                driverClass = getClass().getClassLoader().loadClass(driverClassName);
                log.debug("Driver class {} found in the HikariConfig class classloader {}",
                        driverClassName, getClass().getClassLoader());
            }
        } catch (ClassNotFoundException ex) {
            log.error("Failed to load driver class {} from HikariConfig class classloader {}",
                    driverClassName, getClass().getClassLoader());
        }

        if (driverClass == null) {
            throw new RuntimeException("Failed to load driver class " + driverClassName
                    + " in either of HikariConfig class loader or Thread context classloader");
        }

        try {
            driverClass.getConstructor().newInstance();
            this.driverClassName = driverClassName;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to instantiate class " + driverClassName, ex);
        }
    }

    public void setJdbcUrl(String jdbcUrl) {
        checkIfSealed();
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * Set the default auto-commit behavior of connections in the pool.
     *
     * @param autoCommit the desired auto-commit default for connections
     */
    public void setAutoCommit(boolean autoCommit) {
        checkIfSealed();
        this.autoCommit = autoCommit;
    }

    /**
     * Set whether or not pool suspension is allowed.  There is a performance
     * impact when pool suspension is enabled.  Unless you need it (for a
     * redundancy system for example) do not enable it.
     *
     * @param allowPoolSuspension the desired pool suspension allowance
     */
    public void setAllowPoolSuspension(boolean allowPoolSuspension) {
        checkIfSealed();
        this.allowPoolSuspension = allowPoolSuspension;
    }

    /**
     * Set the pool initialization failure timeout.  This setting applies to pool
     * initialization when {@link HikariDataSource} is constructed with a {@link HikariConfig},
     * or when {@link HikariDataSource} is constructed using the no-arg constructor
     * and {@link HikariDataSource#getConnection()} is called.
     * <ul>
     *   <li>Any value greater than zero will be treated as a timeout for pool initialization.
     *       The calling thread will be blocked from continuing until a successful connection
     *       to the database, or until the timeout is reached.  If the timeout is reached, then
     *       a {@code PoolInitializationException} will be thrown. </li>
     *   <li>A value of zero will <i>not</i>  prevent the pool from starting in the
     *       case that a connection cannot be obtained. However, upon start the pool will
     *       attempt to obtain a connection and validate that the {@code connectionTestQuery}
     *       and {@code connectionInitSql} are valid.  If those validations fail, an exception
     *       will be thrown.  If a connection cannot be obtained, the validation is skipped
     *       and the the pool will start and continue to try to obtain connections in the
     *       background.  This can mean that callers to {@code DataSource#getConnection()} may
     *       encounter exceptions. </li>
     *   <li>A value less than zero will bypass any connection attempt and validation during
     *       startup, and therefore the pool will start immediately.  The pool will continue to
     *       try to obtain connections in the background. This can mean that callers to
     *       {@code DataSource#getConnection()} may encounter exceptions. </li>
     * </ul>
     * Note that if this timeout value is greater than or equal to zero (0), and therefore an
     * initial connection validation is performed, this timeout does not override the
     * {@code connectionTimeout} or {@code validationTimeout}; they will be honored before this
     * timeout is applied.  The default value is one millisecond.
     *
     * @param initializationFailTimeout the number of milliseconds before the
     *                                  pool initialization fails, or 0 to validate connection setup but continue with
     *                                  pool start, or less than zero to skip all initialization checks and start the
     *                                  pool without delay.
     */
    public void setInitializationFailTimeout(long initializationFailTimeout) {
        checkIfSealed();
        this.initializationFailTimeout = initializationFailTimeout;
    }

    /**
     * Configure whether internal pool queries, principally aliveness checks, will be isolated in their own transaction
     * via {@link Connection#rollback()}.  Defaults to {@code false}.
     *
     * @param isolateInternalQueries {@code true} if internal pool queries should be isolated, {@code false} if not
     */
    public void setIsolateInternalQueries(boolean isolateInternalQueries) {
        checkIfSealed();
        this.isolateInternalQueries = isolateInternalQueries;
    }

    public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
        if (metricRegistry != null) {
            throw new IllegalStateException("cannot use setMetricsTrackerFactory() and setMetricRegistry() together");
        }
        this.metricsTrackerFactory = metricsTrackerFactory;
    }

    /**
     * Set a MetricRegistry instance to use for registration of metrics used by HikariCP.
     *
     * @param metricRegistry the MetricRegistry instance to use
     */
    public void setMetricRegistry(Object metricRegistry) {
        if (metricsTrackerFactory != null) {
            throw new IllegalStateException("cannot use setMetricRegistry() and setMetricsTrackerFactory() together");
        }

        if (metricRegistry != null) {
            metricRegistry = getObjectOrPerformJndiLookup(metricRegistry);

            if (!safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")
                    && !(safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry"))) {
                throw new IllegalArgumentException("Class must be instance of com.codahale.metrics.MetricRegistry"
                        + " or io.micrometer.core.instrument.MeterRegistry");
            }
        }

        this.metricRegistry = metricRegistry;
    }

    /**
     * Set the HealthCheckRegistry that will be used for registration of health checks by HikariCP.
     * Currently only Codahale/DropWizard is supported for health checks. Default is {@code null}.
     *
     * @param healthCheckRegistry the HealthCheckRegistry to be used
     */
    public void setHealthCheckRegistry(Object healthCheckRegistry) {
        checkIfSealed();

        if (healthCheckRegistry != null) {
            healthCheckRegistry = getObjectOrPerformJndiLookup(healthCheckRegistry);

            if (!(healthCheckRegistry instanceof HealthCheckRegistry)) {
                throw new IllegalArgumentException("Class must be an instance of"
                        + " com.codahale.metrics.health.HealthCheckRegistry");
            }
        }

        this.healthCheckRegistry = healthCheckRegistry;
    }

    public void setHealthCheckProperties(Properties healthCheckProperties) {
        checkIfSealed();
        this.healthCheckProperties.putAll(healthCheckProperties);
    }

    public void addHealthCheckProperty(String key, String value) {
        checkIfSealed();
        healthCheckProperties.setProperty(key, value);
    }

    /**
     * Configures the Connections to be added to the pool as read-only Connections.
     *
     * @param readOnly {@code true} if the Connections in the pool are read-only, {@code false} if not
     */
    public void setReadOnly(boolean readOnly) {
        checkIfSealed();
        this.readOnly = readOnly;
    }

    /**
     * Configures whether HikariCP self-registers the {@link HikariConfigMXBean} and {@link HikariPoolMXBean} in JMX.
     *
     * @param register {@code true} if HikariCP should register MXBeans, {@code false} if it should not
     */
    public void setRegisterMbeans(boolean register) {
        checkIfSealed();
        registerMbeans = register;
    }

    /**
     * Set the name of the connection pool.  This is primarily used for the MBean
     * to uniquely identify the pool configuration.
     *
     * @param poolName the name of the connection pool to use
     */
    public void setPoolName(String poolName) {
        checkIfSealed();
        this.poolName = poolName;
    }

    /**
     * Set the ScheduledExecutorService used for housekeeping.
     *
     * @param executor the ScheduledExecutorService
     */
    public void setScheduledExecutor(ScheduledExecutorService executor) {
        checkIfSealed();
        scheduledExecutor = executor;
    }

    /**
     * Set the default schema name to be set on connections.
     *
     * @param schema the name of the default schema
     */
    public void setSchema(String schema) {
        checkIfSealed();
        this.schema = schema;
    }

    /**
     * Set the user supplied SQLExceptionOverride class name.
     *
     * @param exceptionOverrideClassName the user supplied SQLExceptionOverride class name
     * @see SQLExceptionOverride
     */
    public void setExceptionOverrideClassName(String exceptionOverrideClassName) {
        checkIfSealed();
        Class<?> overrideClass = attemptFromContextLoader(exceptionOverrideClassName);

        try {
            if (overrideClass == null) {
                overrideClass = getClass().getClassLoader().loadClass(exceptionOverrideClassName);
                log.debug("SQLExceptionOverride class {} found in the HikariConfig class classloader {}",
                        exceptionOverrideClassName, getClass().getClassLoader());
            }
        } catch (ClassNotFoundException ex) {
            log.error("Failed to load SQLExceptionOverride class {} from HikariConfig class classloader {}",
                    exceptionOverrideClassName, getClass().getClassLoader());
        }

        if (overrideClass == null) {
            throw new RuntimeException("Failed to load SQLExceptionOverride class " + exceptionOverrideClassName
                    + " in either of HikariConfig class loader or Thread context classloader");
        }

        try {
            overrideClass.getConstructor().newInstance();
            this.exceptionOverrideClassName = exceptionOverrideClassName;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to instantiate class " + exceptionOverrideClassName, ex);
        }
    }

    /**
     * Set the default transaction isolation level.  The specified value is the
     * constant name from the <code>Connection</code> class, e.g.
     * <code>TRANSACTION_REPEATABLE_READ</code>.
     *
     * @param isolationLevel the name of the isolation level
     */
    public void setTransactionIsolation(String isolationLevel) {
        checkIfSealed();
        transactionIsolation = isolationLevel;
    }

    /**
     * Set the thread factory to be used to create threads.
     *
     * @param threadFactory the thread factory (setting to null causes the default thread factory to be used)
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        checkIfSealed();
        this.threadFactory = threadFactory;
    }

    void seal() {
        sealed = true;
    }

    /**
     * Copies the state of {@code this} into {@code other}.
     *
     * @param other Other {@link HikariConfig} to copy the state to.
     */
    public void copyStateTo(HikariConfig other) {
        for (Field field : HikariConfig.class.getDeclaredFields()) {
            if (!Modifier.isFinal(field.getModifiers())) {
                field.setAccessible(true);

                try {
                    field.set(other, field.get(this));
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to copy HikariConfig state: " + ex.getMessage(), ex);
                }
            }
        }

        other.sealed = false;
    }

    // ***********************************************************************
    //                          Private methods
    // ***********************************************************************

    private @Nullable Class<?> attemptFromContextLoader(String driverClassName) {
        ClassLoader threadContextClassLoader = Thread.currentThread().getContextClassLoader();

        if (threadContextClassLoader != null) {
            try {
                Class<?> driverClass = threadContextClassLoader.loadClass(driverClassName);
                log.debug("Driver class {} found in Thread context class loader {}",
                        driverClassName, threadContextClassLoader);
                return driverClass;
            } catch (ClassNotFoundException ex) {
                log.debug("Driver class {} not found in Thread context class loader {}, trying classloader {}",
                        driverClassName, threadContextClassLoader, getClass().getClassLoader());
            }
        }
        return null;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void validate() {
        if (poolName == null) {
            poolName = generatePoolName();
        } else if (registerMbeans && poolName.contains(":")) {
            throw new IllegalArgumentException("poolName cannot contain ':' when used with JMX");
        }

        // treat empty property as null
        //noinspection NonAtomicOperationOnVolatileField
        catalog = getNullIfEmpty(catalog);
        connectionInitSql = getNullIfEmpty(connectionInitSql);
        connectionTestQuery = getNullIfEmpty(connectionTestQuery);
        transactionIsolation = getNullIfEmpty(transactionIsolation);
        dataSourceClassName = getNullIfEmpty(dataSourceClassName);
        dataSourceJNDI = getNullIfEmpty(dataSourceJNDI);
        driverClassName = getNullIfEmpty(driverClassName);
        jdbcUrl = getNullIfEmpty(jdbcUrl);

        // Check Data Source Options
        if (dataSource != null) {
            if (dataSourceClassName != null) {
                log.warn("{} - using dataSource and ignoring dataSourceClassName.", poolName);
            }
        } else if (dataSourceClassName != null) {
            if (driverClassName != null) {
                log.error("{} - cannot use driverClassName and dataSourceClassName together.", poolName);
                // NOTE: This exception text is referenced by a Spring Boot FailureAnalyzer, it should not be
                // changed without first notifying the Spring Boot developers.
                throw new IllegalStateException("cannot use driverClassName and dataSourceClassName together.");
            } else if (jdbcUrl != null) {
                log.warn("{} - using dataSourceClassName and ignoring jdbcUrl.", poolName);
            }
        } else if (jdbcUrl != null || dataSourceJNDI != null) {
            // ok
        } else if (driverClassName != null) {
            log.error("{} - jdbcUrl is required with driverClassName.", poolName);
            throw new IllegalArgumentException("jdbcUrl is required with driverClassName.");
        } else {
            log.error("{} - dataSource or dataSourceClassName or jdbcUrl is required.", poolName);
            throw new IllegalArgumentException("dataSource or dataSourceClassName or jdbcUrl is required.");
        }

        validateNumerics();

        if (log.isDebugEnabled() || UNIT_TEST) {
            logConfiguration();
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void validateNumerics() {
        maxLifetime = validateValue("maxLifetime", maxLifetime,
                MAX_LIFETIME, MIN_LIFETIME, false);

        keepaliveTime = validateKeepaliveTime();
        leakDetectionThreshold = validateLeakDetectionThreshold();

        connectionTimeout = validateValue("connectionTimeout", connectionTimeout,
                CONNECTION_TIMEOUT, SOFT_TIMEOUT_FLOOR, true);

        validationTimeout = validateValue("validationTimeout", validationTimeout,
                VALIDATION_TIMEOUT, SOFT_TIMEOUT_FLOOR, true);

        maximumPoolSize = Math.max(maximumPoolSize, MINIMUM_POOL_SIZE);

        minimumIdle = validateMinimumIdle();
        idleTimeout = validateIdleTimeout();
    }

    private long validateValue(String propertyName, long currentValue,
                               long defaultValue, long minValue, boolean useDefault) {
        if (currentValue != 0 && currentValue < minValue) {
            log.warn("{} - {} is less than {}ms, setting to {}ms.", poolName, propertyName, minValue, defaultValue);
            return useDefault ? defaultValue : minValue;
        }
        return currentValue;
    }

    private long validateKeepaliveTime() {
        if (keepaliveTime != 0 && keepaliveTime < MIN_KEEPALIVE_TIME) {
            log.warn("{} - keepaliveTime is less than {}ms, disabling it.", poolName, MIN_KEEPALIVE_TIME);
            return DEFAULT_KEEPALIVE_TIME;
        }

        if (keepaliveTime != 0 && maxLifetime != 0 && keepaliveTime >= maxLifetime) {
            log.warn("{} - keepaliveTime is greater than or equal to maxLifetime, disabling it.", poolName);
            return DEFAULT_KEEPALIVE_TIME;
        }
        return keepaliveTime;
    }

    private long validateLeakDetectionThreshold() {
        if (leakDetectionThreshold > 0 && !UNIT_TEST
                && (leakDetectionThreshold < MIN_LEAK_DETECTION_THRESHOLD
                || (leakDetectionThreshold > maxLifetime && maxLifetime > 0))) {
            log.warn("{} - leakDetectionThreshold is less than {}ms or more than maxLifetime,"
                    + " disabling it.", poolName, MIN_LEAK_DETECTION_THRESHOLD);
            return 0;
        }
        return leakDetectionThreshold;
    }

    private int validateMinimumIdle() {
        if (minimumIdle < 0 || minimumIdle > maximumPoolSize) {
            return maximumPoolSize;
        }
        return minimumIdle;
    }

    private long validateIdleTimeout() {
        if (idleTimeout + SECONDS.toMillis(1) > maxLifetime
                && maxLifetime > 0 && minimumIdle < maximumPoolSize) {
            log.warn("{} - idleTimeout is close to or more than maxLifetime, disabling it.", poolName);
            return 0;
        } else if (idleTimeout != 0 && idleTimeout < SECONDS.toMillis(10) && minimumIdle < maximumPoolSize) {
            log.warn("{} - idleTimeout is less than 10000ms, setting to default {}ms.", poolName, IDLE_TIMEOUT);
            return IDLE_TIMEOUT;
        } else if (idleTimeout != IDLE_TIMEOUT && idleTimeout != 0 && minimumIdle == maximumPoolSize) {
            log.warn("{} - idleTimeout has been set but has no effect because"
                    + " the pool is operating as a fixed size pool.", poolName);
        }
        return idleTimeout;
    }

    private void checkIfSealed() {
        if (sealed) {
            throw new IllegalStateException("The configuration of the pool is sealed once started."
                    + " Use HikariConfigMXBean for runtime changes.");
        }
    }

    private void logConfiguration() {
        if (!log.isDebugEnabled()) {
            return; // Skip processing if debug logging is not enabled
        }

        log.debug("{} - configuration:", poolName);
        Set<String> propertyNames = new TreeSet<>(PropertyElf.getPropertyNames(HikariConfig.class));

        for (String prop : propertyNames) {
            try {
                Object value = getMaskedPropertyValue(prop);
                String formattedProp = String.format("%-32s", prop); // Ensure left-aligned & padded to 32 characters
                log.debug("{}{}", formattedProp, value);
            } catch (Exception ignored) {
            }
        }
    }

    private Object getMaskedPropertyValue(String prop) {
        Object value;

        try {
            value = PropertyElf.getProperty(prop, this);

            switch (prop) {
                case "dataSourceProperties":
                    Properties dsProps = PropertyElf.copyProperties(dataSourceProperties);
                    dsProps.setProperty("password", "<masked>");
                    value = dsProps;
                    break;

                case "initializationFailTimeout":
                    value = (initializationFailTimeout == Long.MAX_VALUE) ? "infinite" : value;
                    break;

                case "transactionIsolation":
                    value = (transactionIsolation == null) ? "default" : value;
                    break;

                case "scheduledExecutorService":
                case "threadFactory":
                    value = (value == null) ? "internal" : value;
                    break;

                default:
                    if (prop.contains("jdbcUrl") && value instanceof String) {
                        value = ((String) value).replaceAll("([?&;]password=)[^&#;]*(.*)", "$1<masked>$2");
                    } else if (prop.contains("password")) {
                        value = "<masked>";
                    } else if (value instanceof String) {
                        value = "\"" + value + "\""; // Add quotes for visibility
                    } else if (value == null) {
                        value = "none";
                    }
                    break;
            }
        } catch (Exception ex) {
            value = "error"; // Fallback in case of an error
        }
        return value;
    }

    private void loadProperties(String propertyFileName) {
        Properties props = new Properties();
        Path propFilePath = Paths.get(propertyFileName);

        try (InputStream is = Files.exists(propFilePath)
                ? Files.newInputStream(propFilePath)
                : getClass().getResourceAsStream(propertyFileName)) {
            if (is == null) {
                throw new IllegalArgumentException("Cannot find property file: " + propertyFileName, null);
            }

            props.load(is);
            PropertyElf.setTargetFromProperties(this, props);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to read property file: " + propertyFileName, ex);
        }
    }

    private @NotNull String generatePoolName() {
        try {
            int nextNum = POOL_NUMBER.incrementAndGet();
            return POOL_NAME_PREFIX + nextNum;
        } catch (AccessControlException ex) {
            int randomNum = ThreadLocalRandom.current().nextInt(1, 10000);
            log.info("Assigned random pool name '{}' due to security restrictions.", POOL_NAME_PREFIX + randomNum);
            return POOL_NAME_PREFIX + randomNum;
        }
    }

    private Object getObjectOrPerformJndiLookup(Object object) {
        if (object instanceof String) {
            try {
                InitialContext initCtx = new InitialContext();
                return initCtx.lookup((String) object);
            } catch (NamingException ex) {
                throw new IllegalArgumentException(ex);
            }
        }
        return object;
    }
}
