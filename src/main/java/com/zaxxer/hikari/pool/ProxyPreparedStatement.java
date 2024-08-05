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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This is the proxy class for java.sql.PreparedStatement.
 *
 * @author Brett Wooldridge
 */
public abstract class ProxyPreparedStatement extends ProxyStatement implements PreparedStatement {

    ProxyPreparedStatement(ProxyConnection connection, Statement statement) {
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
