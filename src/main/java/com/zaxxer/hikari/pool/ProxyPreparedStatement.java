package com.zaxxer.hikari.pool;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This is the proxy class for java.sql.PreparedStatement.
 *
 * @author Brett Wooldridge
 */
public abstract class ProxyPreparedStatement extends ProxyStatement implements PreparedStatement {

    ProxyPreparedStatement(ProxyConnection connection, PreparedStatement statement) {
        super(connection, statement);
    }

    // **********************************************************************
    //              Overridden java.sql.PreparedStatement Methods
    // **********************************************************************

    @Override
    public boolean execute() throws SQLException {
        connection.markCommitStateDirty();
        return ((PreparedStatement) delegate).execute();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        connection.markCommitStateDirty();
        ResultSet resultSet = ((PreparedStatement) delegate).executeQuery();
        return ProxyFactory.getProxyResultSet(connection, this, resultSet);
    }

    @Override
    public int executeUpdate() throws SQLException {
        connection.markCommitStateDirty();
        return ((PreparedStatement) delegate).executeUpdate();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        connection.markCommitStateDirty();
        return ((PreparedStatement) delegate).executeLargeUpdate();
    }
}
