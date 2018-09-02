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

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for Tuple.
 *
 * @author kbrockhoff
 */
public class TupleTest {

    @Test
    public void shouldConstructSingleTupleUsingOf() {
        String name = "one";
        Integer value = Integer.valueOf(1);
        Tuple tuple = Tuple.singleOf(name, value);
        assertEquals(1, tuple.size());
        assertEquals(value, tuple.getValue(name));
    }

    @Test
    public void shouldAddElementOnEnd() {
        String name1 = "one";
        Integer value1 = Integer.valueOf(1);
        String name2 = "two";
        Integer value2 = Integer.valueOf(2);
        String name3 = "three";
        Integer value3 = Integer.valueOf(3);
        Tuple tuple = Tuple.EMPTY_TUPLE.addElement(name1, value1).addElement(name2, value2).addElement(name3, value3);
        assertEquals(3, tuple.size());
        assertEquals(value2, tuple.getValue(name2));
    }

    @Test
    public void shouldSupportDifferentValueTypes() {
        String[] names = { "a", "b", "c", "d" };
        Object[] values = { "TEST", new Timestamp(System.currentTimeMillis()), new BigDecimal("8.00"), 732487L };
        Class[] types = { String.class, Timestamp.class, BigDecimal.class, Long.class };
        Tuple tuple = Tuple.EMPTY_TUPLE;
        for (int i = 0; i < names.length; i++) {
            tuple = tuple.addElement(names[i], values[i]);
        }
        List<Class> fieldTypes = tuple.getFieldTypes();
        for (int i = 0; i < types.length; i++) {
            assertEquals(types[i], fieldTypes.get(i));
        }
    }

    @Test
    public void shouldConsiderTuplesWithSameValuesAsEqual() {
        String[] names = { "a", "b", "c", "d" };
        Object[] values = { "TEST", new Timestamp(System.currentTimeMillis()), new BigDecimal("8.00"), null };
        Tuple tuple1 = Tuple.EMPTY_TUPLE;
        Tuple tuple2 = Tuple.EMPTY_TUPLE;
        for (int i = 0; i < names.length; i++) {
            tuple1 = tuple1.addElement(names[i], values[i]);
            tuple2 = tuple2.addElement(names[i], values[i]);
        }
        assertEquals(tuple1, tuple2);
        assertEquals(tuple1.hashCode(), tuple2.hashCode());
    }

}