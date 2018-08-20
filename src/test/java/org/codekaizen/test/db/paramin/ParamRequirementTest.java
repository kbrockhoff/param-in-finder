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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Unit tests for ParamRequirement.
 *
 * @author kbrockhoff
 */
public class ParamRequirementTest {

    @Test
    public void shouldConstructRequirementWithAcceptableValuesList() {
        List<String> acceptable = Arrays.asList("administrator", "poweruser");
        ParamRequirement<String> requirement = ParamRequirement.builder(String.class)
                .setSchema("public").setTable("users").setColumn("usertype")
                .setAcceptor(Acceptors.getValidListAcceptor(acceptable)).build();
        acceptable.forEach(val -> assertTrue(requirement.isAcceptableValue(val)));
        assertFalse(requirement.isAcceptableValue("guest"));
    }

    @Test
    public void shouldConstructRequirementWithMinAndMax() {
        Instant when = Instant.parse("2018-08-12T00:00:00Z");
        Timestamp min = new Timestamp(when.toEpochMilli());
        Timestamp max = new Timestamp(when.plus(Duration.ofDays(7L)).toEpochMilli());
        ParamRequirement<Timestamp> requirement = ParamRequirement.builder(Timestamp.class)
                .setTable("users").setColumn("active_ts")
                .setAcceptor(Acceptors.getMinMaxAcceptor(min, max)).build();
        assertTrue(requirement.isAcceptableValue(min));
        assertFalse(requirement.isAcceptableValue(max));
        assertTrue(requirement.isAcceptableValue(new Timestamp(when.plus(Duration.ofDays(2)).toEpochMilli())));
        assertFalse(requirement.isAcceptableValue(new Timestamp(when.minus(Duration.ofDays(4)).toEpochMilli())));
    }

    @Test
    public void shouldConstructRequirementWithNoRestrictions() {
        ParamRequirement<BigDecimal> requirement = ParamRequirement.builder(BigDecimal.class)
                .setTable("users").setColumn("employee_id").build();
        assertTrue(requirement.isAcceptableValue(new BigDecimal("234879892")));
        assertTrue(requirement.isAcceptableValue(new BigDecimal("18.44")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionOnUnsupportedType() {
        ParamRequirement<DayOfWeek> requirement = ParamRequirement.builder(DayOfWeek.class)
                .setTable("users").setColumn("best_day").build();
        assertTrue(requirement.isAcceptableValue(DayOfWeek.FRIDAY));
    }

    @Test
    public void shouldConstructRequirementWithRegexAcceptor() {
        ParamRequirement<String> requirement = ParamRequirement.builder(String.class)
                .setSchema("public").setTable("users").setWhere("usertype=\'poweruser\'").setColumn("username")
                .setAcceptor(Acceptors.getRegexStringAcceptor(Pattern.compile("^[ABCabc].+"))).build();
        assertTrue(requirement.isAcceptableValue("antman"));
        assertFalse(requirement.isAcceptableValue("scarletwidow"));
    }

    @Test
    public void shouldUtilizeCustomAcceptorIfProvided() {
        ParamRequirement<Integer> requirement = ParamRequirement.builder(Integer.class)
                .setTable("users").setColumn("ranking").setAcceptor(v -> v.compareTo(8) < 0).build();
        assertTrue(requirement.isAcceptableValue(4));
        assertFalse(requirement.isAcceptableValue(16));
    }

}
