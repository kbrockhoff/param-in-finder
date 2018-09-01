/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy singleOf the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.codekaizen.test.db.paramin;

import javax.inject.Inject;
import javax.inject.Named;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.codekaizen.test.db.paramin.Preconditions.*;

/**
 * Queries the RDBMS to find stored procedure in parameters which will result in
 * cursor results matching the specified criteria.
 *
 * @author kbrockhoff
 */
@Named("inParameterFinder")
public class InParameterFinder {

    private final DataSource dataSource;
    private String schema;

    /**
     * Constructs a finder instance.
     *
     * @param dataSource the database connection pool
     */
    @Inject
    public InParameterFinder(DataSource dataSource) {
        checkNotNull(dataSource, "dataSource is required parameter");
        this.dataSource = dataSource;
    }

    /**
     * Returns the default database schema name.
     *
     * @return the schema name or <code>null</code>
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Sets the default database schema name.
     *
     * @param schema the schema name or <code>null</code>
     */
    public void setSchema(String schema) {
        this.schema = emptyToNull(schema);
    }

    /**
     * Returns a fully-qualified table name.
     *
     * @param spec the in parameter specification
     * @return the standardized name
     */
    public String constructTableName(ParamSpec spec) {
        return constructTableName((String) spec.getSchema().orElse(getSchema()), spec.getTable());
    }

    /**
     * Returns a fully-qualified table name.
     *
     * @param schema the schema name or <code>null</code> if not applicable
     * @param table the table name
     * @return the standardized name
     */
    public String constructTableName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            builder.append(schema).append('.');
        }
        builder.append(table);
        return builder.toString().toUpperCase();
    }

    public String constructSqlQuery(ParamSpec spec) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT DISTINCT ").append(spec.getColumn()).append(" FROM ").append(constructTableName(spec));
        //spec.getWhere().ifPresent(clause -> builder.append(" WHERE ").append(clause));
        return builder.toString();
    }

    public List<Map<Integer, Object>> findValidParameters(List<ParamSpec> paramList, int minResultSetSize)
            throws SQLException {
        checkArgument(paramList != null && !paramList.isEmpty(), "paramList cannot be empty");
        List<Map<Integer, Object>> results = new ArrayList<>();
        List<String> selectDistincts = new ArrayList<>();
        int index = 0;
        ParamSpec spec = paramList.get(index);
        retrieveValidValues(spec);
        return results;
    }

    private List<Object> retrieveValidValues(ParamSpec spec) throws SQLException {
        List<Object> values = new ArrayList<>();
        String sql = constructSqlQuery(spec);
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String value = rs.getString(1);
                    if (spec.isAcceptableValue(value)) {
                        values.add(value);
                    }
                }
            }
        }
        return values;
    }

    private void retrieveTableRelationshipsIfNotAlreadyInGraph(ParamSpec spec)
            throws SQLException {
        String node = constructTableName(spec);
    }

    private void retrieveTableGraph(String schema, String table)
            throws SQLException {
        String tblName = constructTableName(schema, table);
        try (Connection conn = getConnection()) {
            String catalog = null;
            DatabaseMetaData dm = conn.getMetaData();
            try (ResultSet rs = dm.getImportedKeys(catalog, schema, table)) {
                while (rs.next()) {
                    String fkTblName = constructTableName(rs.getString("FKTABLE_SCHEM"), rs.getString("FKTABLE_NAME"));
                    StringBuilder builder = new StringBuilder();
                    builder.append(tblName).append(".").append(rs.getString("PKCOLUMN_NAME"))
                            .append("=").append(fkTblName).append(rs.getString("FKCOLUMN_NAME"));
                }
            }
            try (ResultSet rs = dm.getExportedKeys(catalog, schema, table)) {
                while (rs.next()) {
                    String pkTblName = constructTableName(rs.getString("PKTABLE_SCHEM"), rs.getString("PKTABLE_NAME"));
                    StringBuilder builder = new StringBuilder();
                    builder.append(pkTblName).append(".").append(rs.getString("PKCOLUMN_NAME"))
                            .append("=").append(tblName).append(rs.getString("FKCOLUMN_NAME"));
                }
            }
        }
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private void findValues(Specs specs) {
    }

}
