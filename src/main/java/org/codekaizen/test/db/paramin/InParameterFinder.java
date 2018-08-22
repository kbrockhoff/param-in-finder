/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;
import com.google.common.graph.ValueGraphBuilder;

import javax.inject.Inject;
import javax.inject.Named;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Queries the RDBMS to find stored procedure in parameters which will result in
 * cursor results matching the specified criteria.
 *
 * @author kbrockhoff
 */
@Named("inParameterFinder")
public class InParameterFinder {

    private final DataSource dataSource;
    private String catalog;
    private String schema;
    private final Map<String, ValueGraph<String, String>> relationshipMap = new HashMap<>();

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
     * Returns the default database catalog name.
     *
     * @return the catalog name or <code>null</code>
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Sets the default database catalog name.
     *
     * @param catalog the catalog name or <code>null</code>
     */
    public void setCatalog(String catalog) {
        this.catalog = emptyToNull(catalog);
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

    public List<Object> findValidParameters(List<ParamSpec> paramList, int minResultSetSize)
            throws SQLException {
        checkArgument(paramList != null && !paramList.isEmpty(), "paramList cannot be empty");
        List<Object> results = new ArrayList<>(paramList.size());
        List<String> selectDistincts = new ArrayList<>();
        StringBuilder fullSql = new StringBuilder();
        for (ParamSpec spec : paramList) {
            ValueGraph<String, String> graph = getTableRelationships((String) spec.getCatalog().orElse(getCatalog()),
                    (String) spec.getSchema().orElse(getSchema()), spec.getTable());
        }
        return results;
    }

    private ValueGraph<String, String> getTableRelationships(String catalog, String schema, String table)
            throws SQLException {
        String key = constructTableName(catalog, schema, table);
        ValueGraph<String, String> graph = relationshipMap.get(key);
        if (graph == null) {
            graph = retrieveTableGraph(catalog, schema, table);
            relationshipMap.put(key, graph);
        }
        return graph;
    }

    private ValueGraph<String, String> retrieveTableGraph(String catalog, String schema, String table)
            throws SQLException {
        String tblName = constructTableName(catalog, schema, table);
        MutableValueGraph<String, String> graph = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
        graph.addNode(tblName);
        try (Connection conn = getConnection()) {
            DatabaseMetaData dm = conn.getMetaData();
            try (ResultSet rs = dm.getImportedKeys(catalog, schema, table)) {
                while (rs.next()) {
                    String fkTblName = constructTableName(rs.getString("FKTABLE_CAT"),
                            rs.getString("FKTABLE_SCHEM"), rs.getString("FKTABLE_NAME"));
                    graph.addNode(fkTblName);
                    StringBuilder builder = new StringBuilder();
                    builder.append(tblName).append(".").append(rs.getString("PKCOLUMN_NAME"))
                            .append("=").append(fkTblName).append(rs.getString("FKCOLUMN_NAME"));
                    graph.putEdgeValue(table, fkTblName, builder.toString());
                }
            }
            try (ResultSet rs = dm.getExportedKeys(catalog, schema, table)) {
                while (rs.next()) {
                    String pkTblName = constructTableName(rs.getString("PKTABLE_CAT"),
                            rs.getString("PKTABLE_SCHEM"), rs.getString("PKTABLE_NAME"));
                    graph.addNode(pkTblName);
                    StringBuilder builder = new StringBuilder();
                    builder.append(pkTblName).append(".").append(rs.getString("PKCOLUMN_NAME"))
                            .append("=").append(tblName).append(rs.getString("FKCOLUMN_NAME"));
                    graph.putEdgeValue(table, pkTblName, builder.toString());
                }
            }
        }
        return graph;
    }

    private String constructTableName(ParamSpec spec) {
        return constructTableName((String) spec.getCatalog().orElse(getCatalog()),
                (String) spec.getSchema().orElse(getSchema()), spec.getTable());
    }

    private String constructTableName(String catalog, String schema, String table) {
        StringBuilder builder = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            builder.append(catalog).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            builder.append(schema).append('.');
        }
        builder.append(table);
        return builder.toString();
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
