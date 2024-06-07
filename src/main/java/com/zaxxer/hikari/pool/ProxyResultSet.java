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

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This is the proxy class for java.sql.ResultSet.
 *
 * @author Brett Wooldridge
 */
@Getter
@SuppressWarnings("unused")
@AllArgsConstructor
public abstract class ProxyResultSet implements ResultSet {

    protected final ProxyConnection connection;
    protected final ProxyStatement statement;
    final ResultSet delegate;

    final SQLException checkException(SQLException ex) {
        return connection.checkException(ex);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegate;
    }

    // **********************************************************************
    //                 Overridden java.sql.ResultSet Methods
    // **********************************************************************

    @Override
    public void updateRow() throws SQLException {
        connection.markCommitStateDirty();
        delegate.updateRow();
    }

    @Override
    public void insertRow() throws SQLException {
        connection.markCommitStateDirty();
        delegate.insertRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        connection.markCommitStateDirty();
        delegate.deleteRow();
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <T> T unwrap(@NotNull Class<T> iface) throws SQLException {
        if (iface.isInstance(delegate)) {
            return (T) delegate;
        } else if (delegate != null) {
            return delegate.unwrap(iface);
        }
        throw new SQLException("Wrapped ResultSet is not an instance of " + iface);
    }
}
