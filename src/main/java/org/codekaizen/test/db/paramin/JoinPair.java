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
 * Wraps a pair of column names for joining to database tables in an SQL query.
 *
 * @author kbrockhoff
 */
public class JoinPair {

    private final String firstTableColumn;
    private final String secondTableColumn;

    public JoinPair(String firstTableColumn, String secondTableColumn) {
        checkNotEmpty(firstTableColumn);
        checkNotEmpty(secondTableColumn);
        this.firstTableColumn = firstTableColumn;
        this.secondTableColumn = secondTableColumn;
    }

    public String getFirstTableColumn() {
        return firstTableColumn;
    }

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

}
