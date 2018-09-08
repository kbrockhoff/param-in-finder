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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.codekaizen.test.db.paramin.Preconditions.*;
import static org.codekaizen.test.db.paramin.Preconditions.isBlank;

/**
 * Holds the specifications for all database input parameters for one query in a test case execution.
 *
 * @author kbrockhoff
 */
public class ParamSpecs {

    /**
     * Constructs a parameter specifications object with one parameter specification.
     *
     * @param firstSpec the requirement for the first parameter
     * @return the specifications
     */
    public static ParamSpecs create(ParamSpec<?> firstSpec) {
        return new ParamSpecs(firstSpec);
    }

    private Logger logger = LoggerFactory.getLogger(ParamSpecs.class);
    private String schema;
    private Node first;
    private Node last;

    private ParamSpecs(ParamSpec firstSpec) {
        checkNotNull(firstSpec);
        Node node = new Node(null, new JoinPair[0], firstSpec, null);
        first = node;
        last = node;
    }

    /**
     * Adds another parameter specification to the list.
     *
     * @param spec the requirement
     * @param on   the database join columns with the table in the previously added spec; the column in the
     *             previous table should be first in each pair
     * @return this object
     */
    public ParamSpecs join(ParamSpec<?> spec, JoinPair... on) {
        checkNotNull(spec);
        checkArgument(on.length > 0, "at least one join column must be supplied");
        Node node = new Node(last, on, spec, null);
        last.next = node;
        last = node;
        return this;
    }

    /**
     * Returns the default database schema name.
     *
     * @return the schema name or <code>null</code>
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Sets the default database schema name.
     *
     * @param schema the schema name or <code>null</code>
     */
    public void setSchema(String schema) {
        this.schema = emptyToNull(schema);
    }

    /**
     * Returns the parameter requirements in defined order.
     *
     * @return the parameter specifications
     */
    public List<ParamSpec<?>> getParamSpecs() {
        List<ParamSpec<?>> specs = new ArrayList<>();
        Node node = first;
        while (node != null) {
            specs.add(node.item);
            node = node.next;
        }
        return specs;
    }

    /**
     * Returns the SQL statement needed to retrieve values for the provided specification. It includes the query
     * parameters for previous parameter specifications.
     *
     * @param spec the spec retrieve values for
     * @return the SQL parameterized query
     */
    public String getSqlStatement(ParamSpec<?> spec) {
        checkNotNull(spec);
        StringJoiner columns = new StringJoiner(", ");
        StringBuilder tables = new StringBuilder();
        StringJoiner where = new StringJoiner(" AND ");
        Node node = first;
        char alias = 'a';
        while (node != null) {
            String aliasStr = alias + ".";
            if (tables.length() == 0) {
                tables.append(constructTableName(node.item)).append(" ").append(alias);
            } else {
                tables.append(" INNER JOIN ")
                        .append(constructTableName(node.item)).append(" ").append(alias)
                        .append(" ON ");
                String pAliasStr = ((char) (alias - 1)) + ".";
                for (int i = 0; i < node.on.length; i++) {
                    if (i > 0) {
                        tables.append(" AND ");
                    }
                    tables.append(pAliasStr + node.on[i].getFirstTableColumn()).append('=')
                            .append(aliasStr + node.on[i].getSecondTableColumn());
                }
            }
            node.item.getWhere().forEach(c -> where.add(aliasStr + c));
            if (spec.equals(node.item)) {
                columns.add(aliasStr + node.item.getColumn().toLowerCase());
                break;
            } else {
                where.add(aliasStr + node.item.getColumn().toLowerCase() + " = ?");
            }
            node = node.next;
            alias++;
        }
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(columns).append(" FROM ").append(tables);
        if (where.length() > 0) {
            builder.append(" WHERE ").append(where);
        }
        String sql = builder.toString();
        logger.debug("constructed: {}", sql);
        return sql;
    }

    private String constructTableName(ParamSpec spec) {
        return constructTableName((String) spec.getSchema().orElse(getSchema()), spec.getTable());
    }

    private String constructTableName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        if (!isBlank(schema)) {
            builder.append(schema).append('.');
        }
        builder.append(table);
        return builder.toString().toLowerCase();
    }

    private static class Node {

        Node prev;
        JoinPair[] on;
        ParamSpec<?> item;
        Node next;

        Node(Node prev, JoinPair[] on, ParamSpec<?> item, Node next) {
            this.prev = prev;
            this.on = on;
            this.item = item;
            this.next = next;
        }

    }

}
