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

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegate;
    }

    // **********************************************************************
    //                 Overridden java.sql.ResultSet Methods
    // **********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateRow() throws SQLException {
        connection.markCommitStateDirty();
        delegate.updateRow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertRow() throws SQLException {
        connection.markCommitStateDirty();
        delegate.insertRow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteRow() throws SQLException {
        connection.markCommitStateDirty();
        delegate.deleteRow();
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
        throw new SQLException("Wrapped ResultSet is not an instance of " + iface);
    }
}
