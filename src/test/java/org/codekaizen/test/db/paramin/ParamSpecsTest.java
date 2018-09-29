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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit tests for ParamSpecs.
 *
 * @author kbrockhoff
 */
public class ParamSpecsTest {

    private final Logger logger = LoggerFactory.getLogger(ParamSpecsTest.class);

    @Test
    public void shouldCreateSpecsWithSingleSpec() {
        ParamSpecs specs = ParamSpecs.create(
                ParamSpec.find(String.class).inColumn("user_id").fromTable("users")
                        .where(new Condition("status", Operator.EQUALS, "ACTIVE")).build());
        specs.inSchema("public");
        List<ParamSpec<?>> results = specs.getParamSpecs();
        logger.info("{}", results);
        assertEquals(1, results.size());
        List<String> statements = results.stream()
                .map(spec -> specs.getSqlStatement(spec))
                .collect(Collectors.toList());
        for (String sql : statements) {
            logger.info(sql);
            assertTrue(sql.startsWith("SELECT "));
            assertTrue(sql.contains(" FROM "));
            assertTrue(sql.contains(" WHERE "));
        }
        assertEquals(1, statements.size());
    }

    @Test
    public void shouldCreateSpecsWithSingleSpecWithNoWhereClause() {
        ParamSpecs specs = ParamSpecs.create(
                ParamSpec.find(String.class).inColumn("user_id").fromTable("users").build());
        specs.inSchema("public");
        List<ParamSpec<?>> results = specs.getParamSpecs();
        logger.info("{}", results);
        assertEquals(1, results.size());
        List<String> statements = results.stream()
                .map(spec -> specs.getSqlStatement(spec))
                .collect(Collectors.toList());
        for (String sql : statements) {
            logger.info(sql);
            assertTrue(sql.startsWith("SELECT "));
            assertTrue(sql.contains(" FROM "));
            assertFalse(sql.contains(" WHERE "));
        }
        assertEquals(1, statements.size());
    }

    @Test
    public void shouldCreateSpecsWithMultipleSpecs() {
        ParamSpecs specs = ParamSpecs.create(
                ParamSpec.find(String.class).inColumn("username").fromTable("users")
                        .where(new Condition("status", Operator.EQUALS, "ACTIVE")).build())
                .join(ParamSpec.find(String.class).inColumn("name").fromTable("groups").build(),
                        new JoinPair("group_id", "id"))
                .join(ParamSpec.find(String.class).inColumn("name").fromTable("roles")
                                .matching(Matchers.newValidListAcceptor(Arrays.asList("administrator", "operator")))
                                .build(),
                        new JoinPair("role_id", "id"));
        List<ParamSpec<?>> results = specs.getParamSpecs();
        logger.info("{}", results);
        assertEquals(3, results.size());
        List<String> statements = results.stream()
                .map(spec -> specs.getSqlStatement(spec))
                .collect(Collectors.toList());
        int index = 0;
        for (String sql : statements) {
            logger.info(sql);
            assertTrue(sql.startsWith("SELECT "));
            assertTrue(sql.contains(" FROM "));
            if (index > 0) {
                assertTrue(sql.contains(" INNER JOIN "));
                assertTrue(sql.contains(" WHERE "));
            }
            index++;
        }
        assertEquals(3, statements.size());
    }

    @Test
    public void shouldStoreDesiredTuplesSetSize() {
        int desiredTuplesSetSize = 8;
        String schema = "idm";
        ParamSpecs specs = ParamSpecs.create(
                ParamSpec.find(String.class).inColumn("user_id").fromTable("users")
                        .where(new Condition("status", Operator.EQUALS, "ACTIVE")).build())
                        .retrieveTuplesSetOfSize(desiredTuplesSetSize)
                        .inSchema(schema);
        assertEquals(desiredTuplesSetSize, specs.getDesiredTuplesSetSize());
        assertEquals(schema, specs.getSchema());
    }

}