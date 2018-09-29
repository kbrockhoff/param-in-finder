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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.codekaizen.test.db.paramin.ParamSpec.find;
import static org.codekaizen.test.db.paramin.ParamSpecs.create;
import static org.codekaizen.test.db.paramin.Preconditions.isBlank;
import static org.junit.Assert.*;

/**
 * Unit tests for FindParametersExecutor.
 *
 * @author kbrockhoff
 */
public class FindParametersExecutorTest {

    private final Logger logger = LoggerFactory.getLogger(FindParametersExecutorTest.class);
    private Server server;
    private DataSource dataSource;
    private FindParametersExecutor findParametersExecutor;

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
        findParametersExecutor = new FindParametersExecutor(dataSource);
    }

    public void tearDown() {
        if (findParametersExecutor != null) {
            findParametersExecutor.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void shouldFindValidParametersInSingleTable() throws Exception {
        int size = 2;
        ParamSpecs paramSpecs = create(find(String.class).fromTable("specialties").inColumn("name").build())
                .retrieveTuplesSetOfSize(size);
        Future<Set<Tuple>> future = findParametersExecutor.findValidParameters(paramSpecs);
        Set<Tuple> results = future.get();
        results.forEach(t -> logger.info("{}", t));
        assertEquals(size, results.size());
    }

    @Test
    public void shouldFindValidParametersAcrossJoinedTables() throws Exception {
        int size = 12;
        ParamSpecs paramSpecs = create(find(String.class).fromTable("types").inColumn("name").build())
                .join(find(String.class).fromTable("pets").inColumn("id").build(), new JoinPair("id", "type_id"))
                .join(find(String.class).fromTable("owners").inColumn("city").build(), new JoinPair("owner_id", "id"))
                .retrieveTuplesSetOfSize(size);
        Future<Set<Tuple>> future = findParametersExecutor.findValidParameters(paramSpecs);
        Set<Tuple> results = future.get();
        results.forEach(t -> logger.info("{}", t));
        assertEquals(size, results.size());
    }

    @Test
    public void shouldFindAsManyValidParametersAsPossibleOnSingleTableBeforeThrowingException() throws Exception {
        int size = 4;
        ParamSpecs paramSpecs = create(find(String.class).fromTable("specialties").inColumn("name").build())
                .retrieveTuplesSetOfSize(size);
        Future<Set<Tuple>> future = findParametersExecutor.findValidParameters(paramSpecs);
        try {
            Set<Tuple> results = future.get();
            fail("should have thrown exception");
        } catch (ExecutionException exception) {
            assertTrue(exception.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void shouldFindValidParametersOnJoinedTablesBeforeThrowingException() throws Exception {
        int size = 16;
        ParamSpecs paramSpecs = create(find(String.class).fromTable("types").inColumn("name").build())
                .join(find(String.class).fromTable("pets").inColumn("id").build(), new JoinPair("id", "type_id"))
                .join(find(String.class).fromTable("owners").inColumn("city").build(), new JoinPair("owner_id", "id"))
                .retrieveTuplesSetOfSize(size);
        Future<Set<Tuple>> future = findParametersExecutor.findValidParameters(paramSpecs);
        try {
            Set<Tuple> results = future.get();
            fail("should have thrown exception");
        } catch (ExecutionException exception) {
            assertTrue(exception.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void shouldFindValidParametersWithSameFirstParameterValue() throws Exception {
        int size = 4;
        String petType = "dog";
        ParamSpecs paramSpecs = create(find(String.class).fromTable("types").inColumn("name")
                .where(new Condition("name", Operator.EQUALS, petType)).build())
                .join(find(String.class).fromTable("pets").inColumn("id").build(), new JoinPair("id", "type_id"))
                .join(find(String.class).fromTable("owners").inColumn("city").build(), new JoinPair("owner_id", "id"))
                .retrieveTuplesSetOfSize(size);
        Future<Set<Tuple>> future = findParametersExecutor.findValidParameters(paramSpecs);
        Set<Tuple> results = future.get();
        results.forEach(t -> logger.info("{}", t));
        assertEquals(size, results.size());
        results.forEach(t -> assertEquals(petType, t.getValue(0)));
    }

    private void createAndLoadDatabase() throws SQLException, IOException {
        final List<String> schemaStmts = new ArrayList<>();
        final List<String> dataStmts = new ArrayList<>();
        try (InputStream stream = getClass().getResourceAsStream("/db/hsqldb/schema.sql");
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            final List<String> lines = reader.lines()
                    .map(line -> line.trim())
                    .filter(line -> !isBlank(line))
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
                    .filter(line -> !isBlank(line))
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