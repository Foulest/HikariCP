package com.zaxxer.hikari.pool;

import lombok.AllArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.sql.*;

@SuppressWarnings("unused")
@AllArgsConstructor
public abstract class ProxyDatabaseMetaData implements DatabaseMetaData {

    protected final ProxyConnection connection;
    protected final DatabaseMetaData delegate;

    final SQLException checkException(SQLException ex) {
        return connection.checkException(ex);
    }

    @Override
    public final @NotNull String toString() {
        String delegateToString = delegate.toString();
        return getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegateToString;
    }

    // **********************************************************************
    //                 Overridden java.sql.DatabaseMetaData Methods
    // **********************************************************************

    @Override
    public final Connection getConnection() {
        return connection;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern,
                                   String procedureNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getProcedures(catalog, schemaPattern, procedureNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern, String columnNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getProcedureColumns(catalog,
                schemaPattern, procedureNamePattern, columnNamePattern);

        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern, String[] types) throws SQLException {
        ResultSet resultSet = delegate.getTables(catalog, schemaPattern, tableNamePattern, types);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        ResultSet resultSet = delegate.getSchemas();
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        ResultSet resultSet = delegate.getCatalogs();
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        ResultSet resultSet = delegate.getTableTypes();
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern, String columnNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema,
                                         String table, String columnNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getColumnPrivileges(catalog, schema, table, columnNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table,
                                          int scope, boolean nullable) throws SQLException {
        ResultSet resultSet = delegate.getBestRowIdentifier(catalog, schema, table, scope, nullable);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        ResultSet resultSet = delegate.getVersionColumns(catalog, schema, table);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet resultSet = delegate.getPrimaryKeys(catalog, schema, table);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet resultSet = delegate.getImportedKeys(catalog, schema, table);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        ResultSet resultSet = delegate.getExportedKeys(catalog, schema, table);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                       String parentTable, String foreignCatalog,
                                       String foreignSchema, String foreignTable) throws SQLException {
        ResultSet resultSet = delegate.getCrossReference(parentCatalog, parentSchema,
                parentTable, foreignCatalog, foreignSchema, foreignTable);

        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        ResultSet resultSet = delegate.getTypeInfo();
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique, boolean approximate) throws SQLException {
        ResultSet resultSet = delegate.getIndexInfo(catalog, schema, table, unique, approximate);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern,
                             String typeNamePattern, int[] types) throws SQLException {
        ResultSet resultSet = delegate.getUDTs(catalog, schemaPattern, typeNamePattern, types);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                   String typeNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getSuperTypes(catalog, schemaPattern, typeNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern,
                                    String tableNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getSuperTables(catalog, schemaPattern, tableNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern,
                                   String typeNamePattern, String attributeNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        ResultSet resultSet = delegate.getSchemas(catalog, schemaPattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        ResultSet resultSet = delegate.getClientInfoProperties();
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern,
                                  String functionNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getFunctions(catalog, schemaPattern, functionNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern, String columnNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getFunctionColumns(catalog,
                schemaPattern, functionNamePattern, columnNamePattern);

        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                      String tableNamePattern, String columnNamePattern) throws SQLException {
        ResultSet resultSet = delegate.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        Statement statement = resultSet.getStatement();

        if (statement != null) {
            statement = ProxyFactory.getProxyStatement(connection, statement);
        }
        return ProxyFactory.getProxyResultSet(connection, (ProxyStatement) statement, resultSet);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <T> T unwrap(@NotNull Class<T> iface) throws SQLException {
        if (iface.isInstance(delegate)) {
            return (T) delegate;
        } else if (delegate != null) {
            return delegate.unwrap(iface);
        }
        throw new SQLException("Wrapped DatabaseMetaData is not an instance of " + iface);
    }
}
