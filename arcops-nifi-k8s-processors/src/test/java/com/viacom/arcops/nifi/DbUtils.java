package com.viacom.arcops.nifi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DbUtils {

    private final static Logger LOG = LoggerFactory.getLogger(DbUtils.class);

    public static DbQuery dbQuery(Connection connection, String name, String query, Object... parameters) {
        return new DbQuery(connection, name, query, parameters);
    }

    @FunctionalInterface
    public interface SqlCallback<R, S> {
        R apply(S param) throws SQLException;
    }

    public static class DbQuery {
        private final String name;
        private final String query;
        private final Object[] parameters;
        private final Connection connection;

        DbQuery(Connection connection, String name, String query, Object... parameters) {
            this.name = name;
            this.query = query;
            this.parameters = parameters;
            this.connection = connection;
        }

        private void setParam(CallableStatement stmt, int paramNr, Object param) throws SQLException {
            stmt.setObject(paramNr, param);
        }

        public <T> T call(SqlCallback<T, ResultSet> callback) {
            return customCall(stmt -> {
                setParameters(stmt);

                try (ResultSet rs = stmt.executeQuery()) {
                    // by default ResultSet is not scrollable, so logDBResponse() will throw exception at the end
                    // Apparently MySQL driver returns scrollable ResultSet, but H2 database used in test doesn't,
                    // so we need to check that logDBResponse can be safely called.
                    if (LOG.isTraceEnabled() && (stmt.getResultSetType() == ResultSet.TYPE_SCROLL_INSENSITIVE || stmt.getResultSetType() == ResultSet.TYPE_SCROLL_SENSITIVE )) {
                        logDBResponse(rs);
                    }

                    return callback.apply(rs);
                }
            });
        }

        public <T> T customCall(SqlCallback<T, CallableStatement> callback) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}: query={}, parameters={}", name, query, Arrays.toString(parameters));
            }

            try (Connection conn = connection;
                 CallableStatement stmt = conn.prepareCall(query)) {
                return callback.apply(stmt);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to call database query", e);
            }
        }

        private void setParameters(CallableStatement stmt) throws SQLException {
            for (int i = 0; i < parameters.length; i++) {
                setParam(stmt, i+1, parameters[i]);
            }
        }

        private void logDBResponse(ResultSet rs) throws SQLException {
            ResultSetMetaData rsMetaData = rs.getMetaData();
            int colCount = rsMetaData.getColumnCount();

            String[] colNames = new String[colCount];
            for (int i = 1; i <= colCount; i++) {
                colNames[i-1] = rsMetaData.getColumnName(i);
            }

            List<String[]> values = new ArrayList<>();
            values.add(colNames);

            while (rs.next()) {
                String[] row = new String[colCount];
                for (int i = 1; i <= colCount; i++) {
                    row[i-1] = rs.getString(i);
                }
                values.add(row);
            }

            rs.beforeFirst();
            LOG.trace("{}: result={}", name, Arrays.deepToString(values.toArray()));
        }

    }
}
