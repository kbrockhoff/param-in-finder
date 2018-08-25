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
import org.h2.tools.Server;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.junit.Assert.*;

/**
 * Unit tests for InParameterFinder.
 *
 * @author kbrockhoff
 */
public class InParameterFinderTest {

    private final Logger logger = LoggerFactory.getLogger(InParameterFinderTest.class);
    private Server server;
    private DataSource dataSource;
    private InParameterFinder parameterFinder;

    @Before
    public void setUp() throws SQLException, IOException {
        server = Server.createTcpServer();
        server.start();
        JdbcDataSource candidate = new JdbcDataSource();
        candidate.setURL("jdbc:h2:tcp://localhost/~/test");
        candidate.setUser("sa");
        candidate.setPassword("");
        dataSource = candidate;
        createAndLoadDatabase();
        parameterFinder = new InParameterFinder(dataSource);
        parameterFinder.setCatalog("TEST");
        parameterFinder.setSchema("PUBLIC");
    }

    public void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void findValidParameters() throws SQLException {
        List<ParamSpec> paramList = new ArrayList<>();
        paramList.add(ParamSpec.builder(String.class).setTable("types").setColumn("name").build());
        paramList.add(ParamSpec.builder(String.class).setTable("owners").setColumn("city").build());
        List<Object> results = parameterFinder.findValidParameters(paramList, null, 1);
        assertTrue(results.isEmpty());
    }

    private void createAndLoadDatabase() throws SQLException, IOException {
        final List<String> schemaStmts = new ArrayList<>();
        final List<String> dataStmts = new ArrayList<>();
        try (InputStream stream = getClass().getResourceAsStream("/db/hsqldb/schema.sql");
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            final List<String> lines = reader.lines()
                    .map(line -> line.trim())
                    .filter(line -> !isNullOrEmpty(line))
                    .collect(Collectors.toList());
            String sql = null;
            for (String line : lines) {
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
        }
        try (InputStream stream = getClass().getResourceAsStream("/db/hsqldb/data.sql");
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            final List<String> lines = reader.lines()
                    .map(line -> line.trim())
                    .filter(line -> !isNullOrEmpty(line))
                    .collect(Collectors.toList());
            String sql = null;
            for (String line : lines) {
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
        }
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            for (String schemaStmt : schemaStmts) {
                stmt.executeUpdate(schemaStmt);
            }
            for (String dataStmt : dataStmts) {
                assertTrue(stmt.executeUpdate(dataStmt) > 0);
            }
            conn.commit();
        }
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            int counter = 0;
            try (ResultSet rs = metaData.getTables(conn.getCatalog(), null, "%", new String[] {"TABLE", })) {
                while (rs.next()) {
                    counter++;
                    logger.info("{}.{}.{}", rs.getString("TABLE_CAT"), rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
                }
            }
            conn.commit();
            assertEquals(7, counter);
        }
    }

}