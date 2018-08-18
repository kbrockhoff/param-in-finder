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

/**
 * Queries the RDBMS to find stored procedure in parameters which will result in
 * cursor results matching the specified criteria.
 *
 * @author kbrockhoff
 */
public class InParameterFinder {

    private final DataSource dataSource;
    private final Map<String, ValueGraph<String, String>> relationshipMap = new HashMap<>();

    public InParameterFinder(DataSource dataSource) {
        checkNotNull(dataSource, "dataSource is required parameter");
        this.dataSource = dataSource;
    }

    public List<Object> findValidParameters(List<InParamRequirement> paramList, int minResultSetSize) {
        checkArgument(paramList != null && !paramList.isEmpty(), "paramList cannot be empty");
        List<Object> results = new ArrayList<>(paramList.size());
        return results;
    }

    private ValueGraph<String, String> getTableRelationships(String tableName) throws SQLException {
        ValueGraph<String, String> graph = relationshipMap.get(tableName);
        if (graph == null) {
            graph = retrieveTableGraph(tableName);
            relationshipMap.put(tableName, graph);
        }
        return graph;
    }

    private ValueGraph<String, String> retrieveTableGraph(String tableName) throws SQLException {
        MutableValueGraph<String, String> graph = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
        graph.addNode(tableName);
        try (Connection conn = getConnection()) {
            DatabaseMetaData dm = conn.getMetaData();
            try (ResultSet rs = dm.getImportedKeys(null, null, tableName)) {
                while (rs.next()) {
                    String tblName = rs.getString("FKTABLE_NAME");
                    graph.addNode(tblName);
                    graph.putEdgeValue(tableName, tblName, rs.getString("FKCOLUMN_NAME"));
                }
            }
            try (ResultSet rs = dm.getExportedKeys(null, null, tableName)) {
                while (rs.next()) {
                    String tblName = rs.getString("FKTABLE_NAME");
                    graph.addNode(tblName);
                    graph.putEdgeValue(tableName, tblName, rs.getString("FKCOLUMN_NAME"));
                }
            }
        }
        return graph;
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
