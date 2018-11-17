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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.codekaizen.test.db.paramin.Preconditions.*;

/**
 * Enumerates the brands of supported RDBMS's.
 *
 * @author kbrockhoff
 */
public enum Database {

    DB2("^DB2", " FETCH FIRST %1$d ROWS ONLY"),
    DEFAULT("Not Available", " LIMIT %1$d"),
    DERBY("^Apache Derby", " FETCH FIRST %1$d ROWS ONLY"),
    H2("^H2", " LIMIT %1$d"),
    HSQL("^HSQL", " LIMIT %1$d"),
    MYSQL("^(MySQL|MariaDB)", " LIMIT %1$d"),
    ORACLE("^Oracle", " %2$s ROWNUM<=%1$d"),
    POSTGRESQL("^(PostgreSQL|EnterpriseDB)", " LIMIT %1$d"),
    SQL_SERVER("^Microsoft SQL Server", " TOP %1$d"),
    SYBASE("^(Sybase SQL Server|Adaptive Server)", "");

    private final Pattern databaseProductName;
    private final String limitClause;

    private Database(String databaseProductName, String limitClause) {
        this.databaseProductName = Pattern.compile(databaseProductName);
        this.limitClause = limitClause;
    }

    public Pattern getDatabaseProductName() {
        return databaseProductName;
    }

    public String getLimitClause() {
        return limitClause;
    }

    /**
     * Returns the correct database enum value for the supplied string outputted from the JDBC DatabaseMetaData
     * databaseProductName property.
     *
     * @param databaseProductName the product name
     * @return the associated database or DEFAULT if not able to determine
     */
    public static Database getDatabaseForProductName(String databaseProductName) {
        checkNotNull(databaseProductName, "databaseProductName is required");
        Database result = DEFAULT;
        for (Database candidate : values()) {
            Matcher matcher = candidate.getDatabaseProductName().matcher(databaseProductName);
            if (matcher.find()) {
                result = candidate;
                break;
            }
        }
        return result;
    }

}
