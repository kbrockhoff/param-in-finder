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

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for InParameterFinder.
 *
 * @author kbrockhoff
 */
public class InParameterFinderTest {

    private DataSource dataSource;
    private InParameterFinder parameterFinder;

    @Before
    public void setUp() throws SQLException, IOException {
        JdbcDataSource candidate = new JdbcDataSource();
        candidate.setURL("jdbc:h2:mem:testdb");
        candidate.setUser("sa");
        candidate.setPassword("");
        dataSource = candidate;
        createAndLoadDatabase();
        parameterFinder = new InParameterFinder(dataSource);
    }

    @Test
    public void findValidParameters() throws SQLException {
        Connection conn = dataSource.getConnection();
        assertNotNull(conn);
        conn.close();
    }

    private void createAndLoadDatabase() throws SQLException, IOException {
        final List<String> schemaStmts = new ArrayList<>();
        final List<String> dataStmts = new ArrayList<>();
        try (InputStream stream = getClass().getResourceAsStream("/db/hsqldb/schema.sql");
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            String sql = null;
            String line = reader.readLine();
            while (line != null) {
                if (line.length() > 0) {
                    if (sql == null) {
                        sql = line;
                    }
                    else {
                        sql += "\n" + line;
                    }
                    if (line.endsWith(";")) {
                        schemaStmts.add(sql);
                        sql = null;
                    }
                }
                line = reader.readLine();
            }
        }
        try (InputStream stream = getClass().getResourceAsStream("/db/hsqldb/data.sql");
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            String sql = null;
            String line = reader.readLine();
            while (line != null) {
                if (line.length() > 0) {
                    if (sql == null) {
                        sql = line;
                    }
                    else {
                        sql += "\n" + line;
                    }
                    if (line.endsWith(";")) {
                        dataStmts.add(sql);
                        sql = null;
                    }
                }
                line = reader.readLine();
            }
        }
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            for (String schemaStmt : schemaStmts) {
                stmt.execute(schemaStmt);
            }
            for (String dataStmt : dataStmts) {
                stmt.execute(dataStmt);
            }
        }
    }

}