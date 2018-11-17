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

import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for Database.
 *
 * @author kbrockhoff
 */
public class DatabaseTest {

    private final Logger logger = LoggerFactory.getLogger(DatabaseTest.class);

    @Test
    public void shouldCorrectlyIdentifyDatabase() {
        Map<String, Database> testValues = new HashMap<>();
        testValues.put("DB2 UDB for AS/400", Database.DB2);
        testValues.put("DB2/Linux", Database.DB2);
        testValues.put("CUBRID", Database.DEFAULT);
        testValues.put("Apache Derby", Database.DERBY);
        testValues.put("EnterpriseDB", Database.POSTGRESQL);
        testValues.put("HDB", Database.DEFAULT);
        testValues.put("HSQL Database Engine", Database.HSQL);
        testValues.put("H2", Database.H2);
        testValues.put("MariaDB", Database.MYSQL);
        testValues.put("MySQL", Database.MYSQL);
        testValues.put("Oracle", Database.ORACLE);
        testValues.put("PostgreSQL", Database.POSTGRESQL);
        testValues.put("Microsoft SQL Server", Database.SQL_SERVER);
        testValues.put("Sybase SQL Server", Database.SYBASE);
        testValues.put("Adaptive Server Enterprise", Database.SYBASE);
        testValues.put("Adaptive Server Anywhere", Database.SYBASE);
        testValues.put("Informix Dynamic Server", Database.DEFAULT);
        for (Map.Entry<String, Database> entry : testValues.entrySet()) {
            Database result = Database.getDatabaseForProductName(entry.getKey());
            assertEquals(entry.getValue(), result);
        }
    }

    @Test
    public void shouldCreateRowLimitStatementForEachDatabase() {
        Object[] args = new Object[2];
        args[0] = 16;
        args[1] = "WHERE";
        for (Database database : Database.values()) {
            Formatter formatter = new Formatter();
            formatter.format(database.getLimitClause(), args);
            String result = formatter.toString();
            logger.info(result);
            if (!Database.SYBASE.equals(database)) {
                assertTrue(result.contains("16"));
            }
        }
    }

}