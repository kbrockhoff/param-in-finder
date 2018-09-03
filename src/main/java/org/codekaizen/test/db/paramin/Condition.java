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

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.codekaizen.test.db.paramin.Preconditions.checkNotNull;

/**
 * Represents one where clause condition.
 *
 * @author kbrockhoff
 */
public class Condition {

    private final String column;
    private final Operator operator;
    private final Object value;

    /**
     * Constructs a condition object.
     *
     * @param column   the database column name
     * @param operator the SQL operator
     * @param value    the value(s) to apply the operator to
     */
    public Condition(String column, Operator operator, Object value) {
        checkNotNull(column);
        checkNotNull(operator);
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    /**
     * Returns the database column to evaluate.
     *
     * @return the database column name
     */
    public String getColumn() {
        return column;
    }

    /**
     * Returns the SQL operator to apply.
     *
     * @return the SQL operator
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * Returns the values to evaluate.
     *
     * @return the value(s) to apply the operator to
     */
    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Condition condition = (Condition) o;
        return Objects.equals(column, condition.column) &&
                operator == condition.operator &&
                Objects.equals(value, condition.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, operator, value);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(column).append(operator.getSqlString());
        if (value == null) {
            builder.append("NULL");
        } else if (value instanceof Collection) {
            builder.append(((Collection) value).stream()
                    .map(this::formatValue)
                    .collect(Collectors.joining(",", "(", ")")));
        } else {
            builder.append(formatValue(value));
        }
        return builder.toString();
    }

    private String formatValue(Object val) {
        StringBuilder builder = new StringBuilder();
        if (val instanceof String && !isFunctionOrSubselect((String) val)) {
            builder.append("'").append(String.valueOf(val)).append("'");
        } else {
            builder.append(String.valueOf(val));
        }
        return builder.toString();
    }

    private boolean isFunctionOrSubselect(String val) {
        return val.indexOf('(') >= 0 && val.indexOf(')') >= 0;
    }

}
