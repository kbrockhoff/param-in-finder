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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.*;

import static org.codekaizen.test.db.paramin.Preconditions.*;


/**
 * Holds the specification (requirements) for a single database query input parameter.
 *
 * @author kbrockhoff
 */
public class ParamSpec<T extends Comparable<? super T>> {

    /**
     * Instantiates a parameter specification builder for the specified Java type.
     *
     * @param javaType the parameter class
     * @param <T>      the parameter type
     * @return the find
     */
    public static <T extends Comparable<? super T>> Builder<T> find(Class<T> javaType) {
        return new Builder<>(javaType);
    }

    /**
     * Fluent builder for a parameter specification instance.
     *
     * @param <T> the parameter type
     */
    public static class Builder<T extends Comparable<? super T>> {

        private String schema;
        private String table;
        private String column;
        private List<Condition> where = new ArrayList<>();
        private JDBCType sqlType;
        private final Class<T> javaType;
        private Matcher<T> matcher = Matchers.newAllAcceptor();

        private Builder(Class<T> javaType) {
            checkNotNull(javaType, "javaType is required parameter");
            this.javaType = javaType;
            if (String.class.isAssignableFrom(javaType)) {
                sqlType = JDBCType.VARCHAR;
            } else if (BigDecimal.class.isAssignableFrom(javaType)) {
                sqlType = JDBCType.DECIMAL;
            } else if (Integer.class.isAssignableFrom(javaType)) {
                sqlType = JDBCType.INTEGER;
            } else if (Long.class.isAssignableFrom(javaType)) {
                sqlType = JDBCType.BIGINT;
            } else if (Date.class.isAssignableFrom(javaType)) {
                sqlType = JDBCType.DATE;
            } else if (Timestamp.class.isAssignableFrom(javaType)) {
                sqlType = JDBCType.TIMESTAMP;
            } else if (Character.class.isAssignableFrom(javaType)) {
                sqlType = JDBCType.CHAR;
            } else if (Boolean.class.isAssignableFrom(javaType)) {
                sqlType = JDBCType.BOOLEAN;
            } else {
                throw new IllegalArgumentException(javaType.getName() + " is unsupported type");
            }
        }

        /**
         * Sets the name of the database column to find values in.
         *
         * @param column the column name
         * @return this builder
         */
        public Builder inColumn(String column) {
            checkNotEmpty(column, "column is required");
            this.column = column;
            return this;
        }

        /**
         * Sets the name of the database table to find values for.
         *
         * @param table the DB table name
         * @return this builder
         */
        public Builder fromTable(String table) {
            return fromTable(null, table);
        }

        /**
         * Sets the name of the database table to find values for.
         *
         * @param schema the DB schema name
         * @param table  the DB table name
         * @return this builder
         */
        public Builder fromTable(String schema, String table) {
            checkNotEmpty(table, "table is required");
            this.schema = schema;
            this.table = table;
            return this;
        }

        /**
         * Adds a condition to the SQL WHERE clause which should be used to limit considered values.
         *
         * @param condition the where condition
         * @return this builder
         */
        public Builder where(Condition condition) {
            checkNotNull(condition, "condition cannot be null");
            this.where.add(condition);
            return this;
        }

        /**
         * Specifies a filter for further limiting values which meet this specification.
         *
         * @param matcher replacement for the default matcher which allows all values
         * @return this builder
         */
        public Builder matching(Matcher<T> matcher) {
            checkNotNull(matcher, "matcher cannot be null");
            this.matcher = matcher;
            return this;
        }

        /**
         * Constructs the requirement fromTable the values provided to the find or the default if a value
         * is not provided.
         *
         * @return the immutable requirement object
         * @throws IllegalArgumentException if any required values have not been specified
         */
        public ParamSpec<T> build() {
            return new ParamSpec<>(schema, table, column, where, sqlType, javaType, matcher);
        }

    }

    private final String schema;
    private final String table;
    private final String column;
    private final List<Condition> where;
    private final JDBCType sqlType;
    private final Class<T> javaType;
    private final Matcher<T> matcher;

    private ParamSpec(String schema, String table, String column, List<Condition> where,
                      JDBCType sqlType, Class<T> javaType, Matcher<T> matcher) {
        checkArgument(!isBlank(table), "table is required");
        checkArgument(!isBlank(column), "column is required");
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.where = where;
        this.sqlType = sqlType;
        this.javaType = javaType;
        this.matcher = matcher;
    }

    /**
     * Returns the database schema name if it has been specified.
     *
     * @return the schema name if not using the default
     */
    public Optional<String> getSchema() {
        return Optional.ofNullable(schema);
    }

    /**
     * Returns the database table name.
     *
     * @return the table name
     */
    public String getTable() {
        return table;
    }

    /**
     * Returns the database column name.
     *
     * @return the column name
     */
    public String getColumn() {
        return column;
    }

    /**
     * Returns the list of SQL WHERE clause conditions which should be AND'd together.
     *
     * @return the where clause expressions
     */
    public List<Condition> getWhere() {
        return where;
    }

    /**
     * Returns the expected JDBC type of the database column.
     *
     * @return the type
     */
    public JDBCType getSqlType() {
        return sqlType;
    }

    /**
     * Returns the Java type the retrieved values should be converted to.
     *
     * @return the type
     */
    public Class<T> getJavaType() {
        return javaType;
    }

    /**
     * Returns whether the supplied value is determined to be valid using the spec's matcher.
     *
     * @param value the database value to evaluate
     * @return acceptable or not
     */
    public boolean isAcceptableValue(T value) {
        checkNotNull(value, "value is required parameter");
        return matcher.isAcceptableValue(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParamSpec<?> that = (ParamSpec<?>) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(table, that.table) &&
                Objects.equals(column, that.column) &&
                Objects.equals(where, that.where) &&
                sqlType == that.sqlType &&
                Objects.equals(javaType, that.javaType);
    }

    @Override
    public int hashCode() {

        return Objects.hash(schema, table, column, where, sqlType, javaType);
    }

    @Override
    public String toString() {
        return "ParamSpec{" +
                "table='" + table + '\'' +
                ", column='" + column + '\'' +
                '}';
    }

}
