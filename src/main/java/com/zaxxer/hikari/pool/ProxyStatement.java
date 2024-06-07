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
package com.zaxxer.hikari.pool;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This is the proxy class for java.sql.Statement.
 *
 * @author Brett Wooldridge
 */
@SuppressWarnings({"unused", "SqlSourceToSinkFlow"})
public abstract class ProxyStatement implements Statement {

    protected final ProxyConnection connection;
    final Statement delegate;

    private boolean isClosed;
    private ResultSet proxyResultSet;

    ProxyStatement(ProxyConnection connection, Statement statement) {
        this.connection = connection;
        delegate = statement;
    }

    final SQLException checkException(SQLException ex) {
        return connection.checkException(ex);
    }

    @Override
    public final @NotNull String toString() {
        String delegateToString = delegate.toString();
        return getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegateToString;
    }

    // **********************************************************************
    //                 Overridden java.sql.Statement Methods
    // **********************************************************************

    @Override
    public final void close() throws SQLException {
        synchronized (this) {
            if (isClosed) {
                return;
            }

            isClosed = true;
        }

        connection.untrackStatement(delegate);

        try {
            delegate.close();
        } catch (SQLException e) {
            throw connection.checkException(e);
        }
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.execute(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.execute(sql, autoGeneratedKeys);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        connection.markCommitStateDirty();
        ResultSet resultSet = delegate.executeQuery(sql);
        return ProxyFactory.getProxyResultSet(connection, this, resultSet);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeUpdate(sql);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeBatch();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.execute(sql, columnNames);
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeLargeBatch();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeLargeUpdate(sql, columnIndexes);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        connection.markCommitStateDirty();
        return delegate.executeLargeUpdate(sql, columnNames);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet resultSet = delegate.getResultSet();

        if (resultSet != null) {
            if (proxyResultSet == null || ((ProxyResultSet) proxyResultSet).delegate != resultSet) {
                proxyResultSet = ProxyFactory.getProxyResultSet(connection, this, resultSet);
            }
        } else {
            proxyResultSet = null;
        }
        return proxyResultSet;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        ResultSet resultSet = delegate.getGeneratedKeys();

        if (resultSet != null) {
            if (proxyResultSet == null || ((ProxyResultSet) proxyResultSet).delegate != resultSet) {
                proxyResultSet = ProxyFactory.getProxyResultSet(connection, this, resultSet);
            }
        } else {
            proxyResultSet = null;
        }
        return proxyResultSet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <T> T unwrap(@NotNull Class<T> iface) throws SQLException {
        if (iface.isInstance(delegate)) {
            return (T) delegate;
        } else if (delegate != null) {
            return delegate.unwrap(iface);
        }
        throw new SQLException("Wrapped statement is not an instance of " + iface);
    }
}
