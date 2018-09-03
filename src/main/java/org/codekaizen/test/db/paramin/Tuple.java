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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static org.codekaizen.test.db.paramin.Preconditions.checkArgument;
import static org.codekaizen.test.db.paramin.Preconditions.checkNotEmpty;
import static org.codekaizen.test.db.paramin.Preconditions.checkNotNull;

/**
 * Provides an immutable, finite, ordered list with associated names. Used by this library to store
 * results matching parameter specifications.
 *
 * @author kbrockhoff
 */
public class Tuple implements Iterable<Object> {

    /**
     * The 0-tuple.
     */
    public static final Tuple EMPTY_TUPLE = new Tuple(Collections.emptyList(), Collections.emptyList());

    /**
     * Constructs a new single tuple object containing the supplied values.
     *
     * @param name  the name
     * @param value the value which may be null
     * @return the tuple
     * @throws IllegalArgumentException if supplied a blank name
     */
    public static Tuple singleOf(String name, Object value) {
        checkNotEmpty(name);
        List<String> names = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        names.add(name);
        values.add(value);
        return new Tuple(names, values);
    }

    private final List<String> names;
    private final List<Object> values;

    /**
     * Constructs a tuple with the supplied names and values.
     *
     * @param names  the list singleOf names to associate with the values
     * @param values the values
     */
    public Tuple(List<String> names, List<Object> values) {
        checkNotNull(names);
        checkNotNull(values);
        checkArgument(values.size() == names.size(), "Field names must be same length as values");
        this.names = new ArrayList<>(names);
        this.values = new ArrayList<>(values); // shallow copy
    }

    /**
     * Returns a new tuple with the supplied element added to the end singleOf this tuple.
     *
     * @param name  the name
     * @param value the value which may be null
     * @return the tuple
     */
    public Tuple addElement(String name, Object value) {
        checkNotEmpty(name);
        List<String> names = new ArrayList<>(this.names);
        List<Object> values = new ArrayList<>(this.values);
        names.add(name);
        values.add(value);
        return new Tuple(names, values);
    }

    /**
     * Populates a prepared statement's parameters with the values fromTable this tuple.
     *
     * @param statement the prepared statement
     * @throws SQLException if parameterIndex does not correspond to a parameter marker inColumn the SQL statement
     */
    public void populateStatementParameters(PreparedStatement statement) throws SQLException {
        checkNotNull(statement);
        for (int i = 0; i < values.size(); i++) {
            statement.setObject(i + 1, values.get(i));
        }
    }

    /**
     * Populates a callable statement's parameters with the values fromTable this tuple.
     *
     * @param statement the callable statement
     * @throws SQLException if parameterIndex does not correspond to a parameter marker inColumn the SQL statement
     */
    public void populateStatementParameters(CallableStatement statement) throws SQLException {
        checkNotNull(statement);
        for (int i = 0; i < values.size(); i++) {
            statement.setObject(i + 1, values.get(i));
        }
    }

    /**
     * Returns the number singleOf tuple elements.
     *
     * @return the size
     */
    public int size() {
        return values.size();
    }

    /**
     * Returns whether any of the values are {@code null}.
     *
     * @return contains a {@code null} value or not
     */
    public boolean containsNullValue() {
        boolean result = false;
        for (Object value : values) {
            if (value == null) {
                result = true;
            }
        }
        return result;
    }

    /**
     * Returns the list of names for values.
     *
     * @return an unmodifiable list of names
     */
    public List<String> getFieldNames() {
        return Collections.unmodifiableList(names);
    }

    /**
     * Returns the list of contained values.
     *
     * @return an unmodifiable list of values
     */
    public List<Object> getValues() {
        return Collections.unmodifiableList(values);
    }

    /**
     * Returns whether a contained value has the specified name.
     *
     * @param name the field name to check
     * @return present or not
     */
    public boolean hasFieldName(String name) {
        return names.contains(name);
    }

    /**
     * Return the value of the field given the name.
     *
     * @param name the field name
     * @return the value
     * @throws IllegalArgumentException if the name is not present
     */
    public Object getValue(String name) {
        int index = indexOf(name);
        checkArgument(index >= 0, "Field name [" + name + "] does not exist");
        return getValue(index);
    }

    /**
     * Return the value of the field given the index position.
     *
     * @param index the zero-based position
     * @return the value
     * @throws IndexOutOfBoundsException if the index position is out of bounds
     */
    public Object getValue(int index) {
        return values.get(index);
    }

    /**
     * Returns the types of the contained values.
     *
     * @return the classes of the values
     */
    @SuppressWarnings("rawtypes")
    public List<Class> getFieldTypes() {
        ArrayList<Class> types = new ArrayList<>(values.size());
        for (Object val : values) {
            types.add(val.getClass());
        }
        return Collections.unmodifiableList(types);
    }

    /**
     * Returns the tuple values with associated names as a map with the names as the keys.
     *
     * @return the map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            map.put(names.get(i), values.get(i));
        }
        return map;
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }

    @Override
    public int hashCode() {
        return Objects.hash(names, values);
    }

    @Override
    public boolean equals(Object obj) {
        boolean result;
        if (this == obj) {
            result = true;
        } else if (obj instanceof Tuple) {
            Tuple other = (Tuple) obj;
            result = Objects.equals(names, other.names) && Objects.equals(values, other.values);
        } else {
            result = false;
        }
        return result;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "(", ")");
        for (int i = 0; i < values.size(); i++) {
            joiner.add(names.get(i) + ": " + values.get(i));
        }
        return joiner.toString();
    }

    private int indexOf(String name) {
        return names.indexOf(name);
    }

}
