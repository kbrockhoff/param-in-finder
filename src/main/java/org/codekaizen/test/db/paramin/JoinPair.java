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

import java.util.Objects;

import static org.codekaizen.test.db.paramin.Preconditions.checkNotEmpty;

/**
 * Wraps a pair of column names for joining two database tables in an SQL query.
 *
 * @author kbrockhoff
 */
public class JoinPair {

    private final String firstTableColumn;
    private final String secondTableColumn;

    /**
     * Constructs a join pair object.
     *
     * @param firstTableColumn the column in the first table
     * @param secondTableColumn the column in the second table
     */
    public JoinPair(String firstTableColumn, String secondTableColumn) {
        checkNotEmpty(firstTableColumn);
        checkNotEmpty(secondTableColumn);
        this.firstTableColumn = firstTableColumn;
        this.secondTableColumn = secondTableColumn;
    }

    /**
     * Returns the column in the first table to join on.
     *
     * @return the column name
     */
    public String getFirstTableColumn() {
        return firstTableColumn;
    }

    /**
     * Returns the column in the second table to join on.
     *
     * @return the column name
     */
    public String getSecondTableColumn() {
        return secondTableColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinPair joinPair = (JoinPair) o;
        return Objects.equals(firstTableColumn, joinPair.firstTableColumn) &&
                Objects.equals(secondTableColumn, joinPair.secondTableColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstTableColumn, secondTableColumn);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("a.").append(firstTableColumn).append("=b.").append(secondTableColumn);
        return builder.toString();
    }

}
