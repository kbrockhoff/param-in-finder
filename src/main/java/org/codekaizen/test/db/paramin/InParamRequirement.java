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

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.List;
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
public class InParamRequirement<T extends Comparable<? super T>> {

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
        private JDBCType sqlType;
        private final Class<T> javaType;
        private T minValue = null;
        private T maxValue = null;
        private ImmutableList.Builder<T> acceptableValues = ImmutableList.builder();
        private ValueAcceptor<T> acceptor = v -> v != null;

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

        public Builder setMinValue(T minValue) {
            this.minValue = minValue;
            return this;
        }

        public Builder setMaxValue(T maxValue) {
            this.maxValue = maxValue;
            return this;
        }

        public Builder addAcceptableValue(T value) {
            checkNotNull(value, "value is required parameter");
            acceptableValues.add(value);
            return this;
        }

        public Builder setAcceptor(ValueAcceptor<T> acceptor) {
            checkNotNull(acceptor, "acceptor cannot be null");
            this.acceptor = acceptor;
            return this;
        }

        public InParamRequirement<T> build() {
            return new InParamRequirement<>(catalog, schema, table, column, sqlType, javaType, minValue, maxValue,
                    acceptableValues.build(), acceptor);
        }

    }

    private final String catalog;
    private final String schema;
    private final String table;
    private final String column;
    private final JDBCType sqlType;
    private final Class<T> javaType;
    private final T minValue;
    private final T maxValue;
    private final List<T> acceptableValues;
    private final ValueAcceptor<T> acceptor;

    private InParamRequirement(String catalog, String schema, String table, String column, JDBCType sqlType,
                               Class<T> javaType, T minValue, T maxValue, List<T> acceptableValues,
                               ValueAcceptor<T> acceptor) {
        checkArgument(!isNullOrEmpty(table), "table is required");
        checkArgument(!isNullOrEmpty(column), "column is required");
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.sqlType = sqlType;
        this.javaType = javaType;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.acceptableValues = acceptableValues;
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

    public JDBCType getSqlType() {
        return sqlType;
    }

    public Class<T> getJavaType() {
        return javaType;
    }

    public T getMinValue() {
        return minValue;
    }

    public T getMaxValue() {
        return maxValue;
    }

    public List<T> getAcceptableValues() {
        return acceptableValues;
    }

    public boolean isAcceptableValue(T value) {
        checkNotNull(value, "value is required parameter");
        boolean result;
        if (!acceptableValues.isEmpty()) {
            result = acceptableValues.contains(value);
        } else if (minValue != null || maxValue != null) {
            result = true;
            if (minValue != null) {
                result = minValue.compareTo(value) <= 0;
            }
            if (result && maxValue != null) {
                result = maxValue.compareTo(value) >= 0;
            }
        } else {
            result = true;
        }
        if (result) {
            result = acceptor.isAcceptableValue(value);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InParamRequirement<?> that = (InParamRequirement<?>) o;
        return Objects.equals(catalog, that.catalog) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(table, that.table) &&
                Objects.equals(column, that.column) &&
                sqlType == that.sqlType &&
                Objects.equals(javaType, that.javaType) &&
                Objects.equals(minValue, that.minValue) &&
                Objects.equals(maxValue, that.maxValue) &&
                Objects.equals(acceptableValues, that.acceptableValues);
    }

    @Override
    public int hashCode() {

        return Objects.hash(catalog, schema, table, column, sqlType, javaType, minValue, maxValue, acceptableValues);
    }

    @Override
    public String toString() {
        return "InParamRequirement{" +
                "table='" + table + '\'' +
                ", column='" + column + '\'' +
                '}';
    }

}
