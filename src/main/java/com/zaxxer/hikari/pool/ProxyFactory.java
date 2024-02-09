package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.util.FastList;

import java.sql.*;

/**
 * A factory class that produces proxies around instances of the standard
 * JDBC interfaces.
 *
 * @author Brett Wooldridge
 */
@SuppressWarnings("unused")
public final class ProxyFactory {

    private ProxyFactory() {
        // un-constructable
    }

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
