/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except inColumn compliance with the License.
 * You may obtain a copy singleOf the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to inColumn writing, software
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
import java.util.stream.Stream;

import static org.codekaizen.test.db.paramin.Preconditions.*;

/**
 * Queries the RDBMS to find stored procedure inColumn parameters which will result inColumn
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

    public List<Tuple> findValidParameters(ParamSpecs specs, int size) {
        try (Connection conn = getConnection()) {
            specs.getParamSpecs().forEach(spec -> System.out.println(spec));

        } catch (SQLException cause) {
            throw new IllegalStateException(cause);
        }
        return null;
    }

    private Stream<Tuple> retrieveValues(Connection conn, ParamSpecs specs, Tuple previous) {
        ParamSpec spec = specs.getParamSpecs().get(previous.size());
        try (PreparedStatement ps = conn.prepareStatement("")) {
            previous.populateStatementParameters(ps);
            try (ResultSet rs = ps.executeQuery()) {
                Object value = rs.getObject(1);
                if (spec.isAcceptableValue((Comparable<?>) value)) {

                }
            }
        } catch (SQLException cause) {
            throw new IllegalStateException(cause);
        }
        return null;
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
        String sql = "SELECT name FROM types";
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

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
