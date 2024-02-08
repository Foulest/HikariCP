package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.SQLExceptionOverride;
import com.zaxxer.hikari.util.FastList;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.zaxxer.hikari.SQLExceptionOverride.Override.DO_NOT_EVICT;
import static com.zaxxer.hikari.util.ClockSource.currentTime;

/**
 * This is the proxy class for java.sql.Connection.
 *
 * @author Brett Wooldridge
 */
@Getter(lombok.AccessLevel.PACKAGE)
public abstract class ProxyConnection implements Connection {

    static final int DIRTY_BIT_READONLY = 0b000001;
    static final int DIRTY_BIT_AUTOCOMMIT = 0b000010;
    static final int DIRTY_BIT_ISOLATION = 0b000100;
    static final int DIRTY_BIT_CATALOG = 0b001000;
    static final int DIRTY_BIT_NETTIMEOUT = 0b010000;
    static final int DIRTY_BIT_SCHEMA = 0b100000;

    private static final Logger LOGGER;
    private static final Set<String> ERROR_STATES;
    private static final Set<Integer> ERROR_CODES;

    protected Connection delegate;

    private final PoolEntry poolEntry;
    private final ProxyLeakTask leakTask;
    private final FastList<Statement> openStatements;

    private int dirtyBits;
    private long lastAccess;
    private boolean isCommitStateDirty;

    public boolean isReadOnly;
    private boolean isAutoCommit;
    private int networkTimeoutState;
    private int transactionIsolationState;
    private String catalogState;
    private String schemaState;

    // static initializer
    static {
        LOGGER = LoggerFactory.getLogger(ProxyConnection.class);

        ERROR_STATES = new HashSet<>();
        ERROR_STATES.add("0A000"); // FEATURE UNSUPPORTED
        ERROR_STATES.add("57P01"); // ADMIN SHUTDOWN
        ERROR_STATES.add("57P02"); // CRASH SHUTDOWN
        ERROR_STATES.add("57P03"); // CANNOT CONNECT NOW
        ERROR_STATES.add("01002"); // SQL92 disconnect error
        ERROR_STATES.add("JZ0C0"); // Sybase disconnect error
        ERROR_STATES.add("JZ0C1"); // Sybase disconnect error

        ERROR_CODES = new HashSet<>();
        ERROR_CODES.add(500150);
        ERROR_CODES.add(2399);
    }

    protected ProxyConnection(PoolEntry poolEntry,
                              Connection connection,
                              FastList<Statement> openStatements,
                              ProxyLeakTask leakTask,
                              long now,
                              boolean isReadOnly,
                              boolean isAutoCommit) {
        this.poolEntry = poolEntry;
        delegate = connection;
        this.openStatements = openStatements;
        this.leakTask = leakTask;
        lastAccess = now;
        this.isReadOnly = isReadOnly;
        this.isAutoCommit = isAutoCommit;
    }

    // Correct implementation of isReadOnly() as a method
    @Override
    public boolean isReadOnly() throws SQLException {
        // Implementation logic here. For example, delegating the call to the actual connection:
        return delegate.isReadOnly();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final @NotNull String toString() {
        return getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegate;
    }

    // ***********************************************************************
    //                     Connection State Accessors
    // ***********************************************************************

    final boolean getAutoCommitState() {
        return isAutoCommit;
    }

    final boolean getReadOnlyState() {
        return isReadOnly;
    }

    // ***********************************************************************
    //                          Internal methods
    // ***********************************************************************

    @SuppressWarnings("ConstantConditions")
    final SQLException checkException(SQLException ex) {
        boolean evict = false;
        SQLException nse = ex;
        SQLExceptionOverride exceptionOverride = poolEntry.getPoolBase().exceptionOverride;

        for (int depth = 0; delegate != ClosedConnection.CLOSED_CONNECTION && nse != null && depth < 10; depth++) {
            String sqlState = nse.getSQLState();

            if (sqlState != null && sqlState.startsWith("08")
                    || nse instanceof SQLTimeoutException
                    || ERROR_STATES.contains(sqlState)
                    || ERROR_CODES.contains(nse.getErrorCode())) {
                if (exceptionOverride != null && exceptionOverride.adjudicate(nse) == DO_NOT_EVICT) {
                    break;
                }

                // broken connection
                evict = true;
                break;
            } else {
                nse = nse.getNextException();
            }
        }

        if (evict) {
            SQLException exception = (nse != null) ? nse : ex;
            LOGGER.warn("{} - Connection {} marked as broken because of SQLSTATE({}), ErrorCode({})",
                    poolEntry.getPoolName(), delegate, exception.getSQLState(), exception.getErrorCode(), exception);
            leakTask.cancel();
            poolEntry.evict("(connection is broken)");
            delegate = ClosedConnection.CLOSED_CONNECTION;
        }
        return ex;
    }

    final synchronized void untrackStatement(Statement statement) {
        openStatements.remove(statement);
    }

    final void markCommitStateDirty() {
        if (isAutoCommit) {
            lastAccess = currentTime();
        } else {
            isCommitStateDirty = true;
        }
    }

    void cancelLeakTask() {
        leakTask.cancel();
    }

    private synchronized <T extends Statement> T trackStatement(T statement) {
        openStatements.add(statement);
        return statement;
    }

    @SuppressWarnings("EmptyTryBlock")
    private synchronized void closeStatements() {
        int size = openStatements.size();

        if (size > 0) {
            for (int i = 0; i < size && delegate != ClosedConnection.CLOSED_CONNECTION; i++) {
                try (Statement ignored = openStatements.get(i)) {
                    // automatic resource cleanup
                } catch (SQLException ex) {
                    LOGGER.warn("{} - Connection {} marked as broken because of an exception closing"
                            + " open statements during Connection.close()", poolEntry.getPoolName(), delegate);
                    leakTask.cancel();
                    poolEntry.evict("(exception closing Statements during Connection.close())");
                    delegate = ClosedConnection.CLOSED_CONNECTION;
                }
            }

            openStatements.clear();
        }
    }

    // **********************************************************************
    //              "Overridden" java.sql.Connection Methods
    // **********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public final void close() throws SQLException {
        // Closing statements can cause connection eviction, so this must run before the conditional below
        closeStatements();

        if (delegate != ClosedConnection.CLOSED_CONNECTION) {
            leakTask.cancel();

            try {
                if (isCommitStateDirty && !isAutoCommit) {
                    delegate.rollback();
                    lastAccess = currentTime();
                    LOGGER.debug("{} - Executed rollback on connection {} due to dirty"
                            + " commit state on close().", poolEntry.getPoolName(), delegate);
                }

                if (dirtyBits != 0) {
                    poolEntry.resetConnectionState(this, dirtyBits);
                    lastAccess = currentTime();
                }

                delegate.clearWarnings();
            } catch (SQLException ex) {
                // when connections are aborted, exceptions are often thrown that should not reach the application
                if (!poolEntry.isMarkedEvicted()) {
                    throw checkException(ex);
                }
            } finally {
                delegate = ClosedConnection.CLOSED_CONNECTION;
                poolEntry.recycle(lastAccess);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("RedundantThrows")
    public boolean isClosed() throws SQLException {
        return (delegate == ClosedConnection.CLOSED_CONNECTION);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement() throws SQLException {
        return ProxyFactory.getProxyStatement(this,
                trackStatement(delegate.createStatement()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement(int resultSetType, int concurrency) throws SQLException {
        return ProxyFactory.getProxyStatement(this,
                trackStatement(delegate.createStatement(resultSetType, concurrency)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement(int resultSetType, int concurrency, int holdability) throws SQLException {
        return ProxyFactory.getProxyStatement(this,
                trackStatement(delegate.createStatement(resultSetType, concurrency, holdability)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return ProxyFactory.getProxyCallableStatement(this,
                trackStatement(delegate.prepareCall(sql)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int concurrency) throws SQLException {
        return ProxyFactory.getProxyCallableStatement(this,
                trackStatement(delegate.prepareCall(sql, resultSetType, concurrency)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int concurrency, int holdability) throws SQLException {
        return ProxyFactory.getProxyCallableStatement(this,
                trackStatement(delegate.prepareCall(sql, resultSetType, concurrency, holdability)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this,
                trackStatement(delegate.prepareStatement(sql)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this,
                trackStatement(delegate.prepareStatement(sql, autoGeneratedKeys)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this,
                trackStatement(delegate.prepareStatement(sql, resultSetType, concurrency)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int concurrency, int holdability) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this,
                trackStatement(delegate.prepareStatement(sql, resultSetType, concurrency, holdability)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this,
                trackStatement(delegate.prepareStatement(sql, columnIndexes)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return ProxyFactory.getProxyPreparedStatement(this,
                trackStatement(delegate.prepareStatement(sql, columnNames)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        markCommitStateDirty();
        return ProxyFactory.getProxyDatabaseMetaData(this, delegate.getMetaData());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() throws SQLException {
        delegate.commit();
        isCommitStateDirty = false;
        lastAccess = currentTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback() throws SQLException {
        delegate.rollback();
        isCommitStateDirty = false;
        lastAccess = currentTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        delegate.rollback(savepoint);
        isCommitStateDirty = false;
        lastAccess = currentTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        delegate.setAutoCommit(autoCommit);
        isAutoCommit = autoCommit;
        dirtyBits |= DIRTY_BIT_AUTOCOMMIT;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        delegate.setReadOnly(readOnly);
        isReadOnly = readOnly;
        isCommitStateDirty = false;
        dirtyBits |= DIRTY_BIT_READONLY;
    }

    /**
     * {@inheritDoc}
     */
    public void setTransactionIsolationState(int level) throws SQLException {
        delegate.setTransactionIsolation(level);
        transactionIsolationState = level;
        dirtyBits |= DIRTY_BIT_ISOLATION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        delegate.setCatalog(catalog);
        catalogState = catalog;
        dirtyBits |= DIRTY_BIT_CATALOG;
    }

    /**
     * {@inheritDoc}
     */
    public void setNetworkTimeoutState(Executor executor, int milliseconds) throws SQLException {
        delegate.setNetworkTimeout(executor, milliseconds);
        networkTimeoutState = milliseconds;
        dirtyBits |= DIRTY_BIT_NETTIMEOUT;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        delegate.setSchema(schema);
        schemaState = schema;
        dirtyBits |= DIRTY_BIT_SCHEMA;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isWrapperFor(@NotNull Class<?> iface) throws SQLException {
        return iface.isInstance(delegate) || (delegate != null && delegate.isWrapperFor(iface));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public final <T> T unwrap(@NotNull Class<T> iface) throws SQLException {
        if (iface.isInstance(delegate)) {
            return (T) delegate;
        } else if (delegate != null) {
            return delegate.unwrap(iface);
        }
        throw new SQLException("Wrapped connection is not an instance of " + iface);
    }

    // **********************************************************************
    //                         Private classes
    // **********************************************************************

    private static final class ClosedConnection {

        static final Connection CLOSED_CONNECTION = getClosedConnection();

        private static @NotNull Connection getClosedConnection() {
            InvocationHandler handler = (proxy, method, args) -> {
                String methodName = method.getName();

                if ("isClosed".equals(methodName)) {
                    return Boolean.TRUE;
                } else if ("isValid".equals(methodName)) {
                    return Boolean.FALSE;
                }

                if ("abort".equals(methodName)) {
                    return Void.TYPE;
                }

                if ("close".equals(methodName)) {
                    return Void.TYPE;
                } else if ("toString".equals(methodName)) {
                    return ClosedConnection.class.getCanonicalName();
                }
                throw new SQLException("Connection is closed");
            };

            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class[]{Connection.class}, handler);
        }
    }
}
