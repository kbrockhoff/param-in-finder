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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Holds the requirements for one specific input parameter.
 *
 * @author kbrockhoff
 */
public class ParamRequirement<T extends Comparable<? super T>> {

    /**
     * Instantiates a requirement builder for the specified Java type.
     *
     * @param javaType the parameter class
     * @param <T> the parameter type
     * @return the builder
     */
    public static <T extends Comparable<? super T>> Builder<T> builder(Class<T> javaType) {
        return new Builder<>(javaType);
    }

    /**
     * Builder for a requirement instance.
     *
     * @param <T> the parameter type
     */
    public static class Builder<T extends Comparable<? super T>> {

        private String catalog;
        private String schema;
        private String table;
        private String column;
        private String where;
        private JDBCType sqlType;
        private final Class<T> javaType;
        private Acceptor<T> acceptor = Acceptors.getAllAcceptor();

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

        public Builder setCatalog(String catalog) {
            this.catalog = emptyToNull(catalog);
            return this;
        }

        public Builder setSchema(String schema) {
            this.schema = emptyToNull(schema);
            return this;
        }

        public Builder setTable(String table) {
            checkArgument(!isNullOrEmpty(table), "table is required");
            this.table = table;
            return this;
        }

        public Builder setColumn(String column) {
            checkArgument(!isNullOrEmpty(column), "column is required");
            this.column = column;
            return this;
        }

        public Builder setWhere(String where) {
            this.where = emptyToNull(where);
            return this;
        }

        public Builder setAcceptor(Acceptor<T> acceptor) {
            checkNotNull(acceptor, "acceptor cannot be null");
            this.acceptor = acceptor;
            return this;
        }

        /**
         * Constructs the requirement from the values provided to the builder or the default if a value
         * is not provided.
         *
         * @return the immutable requirement object
         * @throws IllegalArgumentException if any required values have not been specified
         */
        public ParamRequirement<T> build() {
            return new ParamRequirement<>(catalog, schema, table, column, where, sqlType, javaType, acceptor);
        }

    }

    private final String catalog;
    private final String schema;
    private final String table;
    private final String column;
    private final String where;
    private final JDBCType sqlType;
    private final Class<T> javaType;
    private final Acceptor<T> acceptor;

    private ParamRequirement(String catalog, String schema, String table, String column, String where,
                             JDBCType sqlType, Class<T> javaType, Acceptor<T> acceptor) {
        checkArgument(!isNullOrEmpty(table), "table is required");
        checkArgument(!isNullOrEmpty(column), "column is required");
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.where = where;
        this.sqlType = sqlType;
        this.javaType = javaType;
        this.acceptor = acceptor;
    }

    public Optional<String> getCatalog() {
        return Optional.ofNullable(catalog);
    }

    public Optional<String> getSchema() {
        return Optional.ofNullable(schema);
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public String getWhere() {
        return where;
    }

    public JDBCType getSqlType() {
        return sqlType;
    }

    public Class<T> getJavaType() {
        return javaType;
    }

    public boolean isAcceptableValue(T value) {
        checkNotNull(value, "value is required parameter");
        return acceptor.isAcceptableValue(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParamRequirement<?> that = (ParamRequirement<?>) o;
        return Objects.equals(catalog, that.catalog) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(table, that.table) &&
                Objects.equals(column, that.column) &&
                Objects.equals(where, that.where) &&
                sqlType == that.sqlType &&
                Objects.equals(javaType, that.javaType);
    }

    @Override
    public int hashCode() {

        return Objects.hash(catalog, schema, table, column, where, sqlType, javaType);
    }

    @Override
    public String toString() {
        return "ParamRequirement{" +
                "table='" + table + '\'' +
                ", column='" + column + '\'' +
                '}';
    }

}
