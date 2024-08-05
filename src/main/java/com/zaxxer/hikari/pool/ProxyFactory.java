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

import com.zaxxer.hikari.util.FastList;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.sql.*;

/**
 * A factory class that produces proxies around instances of the standard
 * JDBC interfaces.
 *
 * @author Brett Wooldridge
 */
@SuppressWarnings("unused")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ProxyFactory {

    /**
     * Create a proxy for the specified {@link Connection} instance.
     *
     * @param poolEntry      the PoolEntry holding pool state
     * @param connection     the raw database Connection
     * @param openStatements a reusable list to track open Statement instances
     * @param leakTask       the ProxyLeakTask for this connection
     * @param now            the current timestamp
     * @param isReadOnly     the default readOnly state of the connection
     * @param isAutoCommit   the default autoCommit state of the connection
     * @return a proxy that wraps the specified {@link Connection}
     */
    static ProxyConnection getProxyConnection(PoolEntry poolEntry, Connection connection,
                                              FastList<Statement> openStatements, ProxyLeakTask leakTask,
                                              long now, boolean isReadOnly, boolean isAutoCommit) {
        // Body is replaced (injected) by JavassistProxyFactory
        throw new IllegalStateException("You need to run the CLI build and"
                + " you need target/classes in your classpath to run.");
    }

    static Statement getProxyStatement(ProxyConnection connection, Statement statement) {
        // Body is replaced (injected) by JavassistProxyFactory
        throw new IllegalStateException("You need to run the CLI build and"
                + " you need target/classes in your classpath to run.");
    }

    static CallableStatement getProxyCallableStatement(ProxyConnection connection, CallableStatement statement) {
        // Body is replaced (injected) by JavassistProxyFactory
        throw new IllegalStateException("You need to run the CLI build and"
                + " you need target/classes in your classpath to run.");
    }

    static PreparedStatement getProxyPreparedStatement(ProxyConnection connection, PreparedStatement statement) {
        // Body is replaced (injected) by JavassistProxyFactory
        throw new IllegalStateException("You need to run the CLI build and"
                + " you need target/classes in your classpath to run.");
    }

    static ResultSet getProxyResultSet(ProxyConnection connection, ProxyStatement statement, ResultSet resultSet) {
        // Body is replaced (injected) by JavassistProxyFactory
        throw new IllegalStateException("You need to run the CLI build and"
                + " you need target/classes in your classpath to run.");
    }

    static DatabaseMetaData getProxyDatabaseMetaData(ProxyConnection connection, DatabaseMetaData metaData) {
        // Body is replaced (injected) by JavassistProxyFactory
        throw new IllegalStateException("You need to run the CLI build and"
                + " you need target/classes in your classpath to run.");
    }
}
