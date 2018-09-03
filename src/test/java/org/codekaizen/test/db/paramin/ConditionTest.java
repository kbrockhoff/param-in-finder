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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for Condition.
 *
 * @author kbrockhoff
 */
public class ConditionTest {

    private final Logger logger = LoggerFactory.getLogger(ConditionTest.class);

    @Test
    public void shouldTranslateStringValuesWithEnclosingQuotes() {
        Condition condition = new Condition("xtype", Operator.EQUALS, "M");
        logger.info("{}", condition);
        assertTrue(condition.toString().contains("'M'"));
    }

    @Test
    public void shouldTranslateSqlFunctionsSuppliedAsStringsWithoutEnclosingQuotes() {
        String value = "TO_DATE('2018-07-04','YYYY-MM-DD')";
        Condition condition = new Condition("begin", Operator.GREATER_THAN_EQUALS, value);
        logger.info("{}", condition);
        assertFalse(condition.toString().contains("'" + value));
    }

    @Test
    public void shouldTranslateCollectionsAsParenthesisEnclosedLists() {
        Condition condition = new Condition("status", Operator.IN, Arrays.asList("SUSPENDED", "DEACTIVATED"));
        logger.info("{}", condition);
        assertTrue(condition.toString().contains("('"));
    }

    @Test
    public void shouldTranslateNullAsSqlNull() {
        Condition condition = new Condition("message", Operator.IS_NOT, null);
        logger.info("{}", condition);
        assertTrue(condition.toString().contains("NULL"));
    }

    @Test
    public void shouldTranslateNumbersAsIs() {
        Condition condition = new Condition("answer", Operator.NOT_EQUALS, 42);
        logger.info("{}", condition);
        assertFalse(condition.toString().contains("'"));
    }

    @Test
    public void shouldTranslateBooleansAsIs() {
        Condition condition = new Condition("active", Operator.IS, true);
        logger.info("{}", condition);
        assertFalse(condition.toString().contains("'"));
    }

    @Test
    public void shouldConsiderConditionsWithSameFieldValuesAsEquals() {
        Condition condition1 = new Condition("last_ret", Operator.IN, Arrays.asList(7, 8, 11));
        Condition condition2 = new Condition("last_ret", Operator.IN, Arrays.asList(7, 8, 11));
        assertEquals(condition1, condition2);
        assertEquals(condition1.hashCode(), condition2.hashCode());
        logger.info("{}", condition1);
        assertEquals(condition1.toString(), condition2.toString());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullColumnName() {
        Condition condition = new Condition(null, Operator.EQUALS, "M");
        logger.info("{}", condition);
        assertTrue(condition.toString().contains("'M'"));
    }

}