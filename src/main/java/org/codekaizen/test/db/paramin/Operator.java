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

/**
 * Ennumerates the supported SQL where clause operators.
 *
 * @author kbrockhoff
 */
public enum Operator {

    EQUALS("="),
    NOT_EQUALS("!="),
    GREATER_THAN(">"),
    LESS_THAN("<"),
    GREATER_THAN_EQUALS(">="),
    LESS_THAN_EQUALS("<="),
    LIKE(" LIKE "),
    IS(" IS "),
    IS_NOT(" IS NOT "),
    IN(" IN "),
    NOT_IN(" NOT IN "),
    ;

    private final String sqlString;

    private Operator(String sqlString) {
        this.sqlString = sqlString;
    }

    /**
     * Returns the string to use when generating SQL statements.
     *
     * @return the SQL representation
     */
    public String getSqlString() {
        return sqlString;
    }

}
