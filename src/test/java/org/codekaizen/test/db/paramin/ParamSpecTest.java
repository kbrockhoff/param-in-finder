/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy singleOf the License at
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
 * Unit tests for ParamSpec.
 *
 * @author kbrockhoff
 */
public class ParamSpecTest {

    @Test
    public void shouldConstructRequirementWithAcceptableValuesList() {
        List<String> acceptable = Arrays.asList("administrator", "poweruser");
        ParamSpec<String> requirement = ParamSpec.builder(String.class)
                .setSchema("public").setTable("users").setColumn("usertype")
                .setMatcher(Matchers.newValidListAcceptor(acceptable)).build();
        acceptable.forEach(val -> assertTrue(requirement.isAcceptableValue(val)));
        assertFalse(requirement.isAcceptableValue("guest"));
    }

    @Test
    public void shouldConstructRequirementWithMinAndMax() {
        Instant when = Instant.parse("2018-08-12T00:00:00Z");
        Timestamp min = new Timestamp(when.toEpochMilli());
        Timestamp max = new Timestamp(when.plus(Duration.ofDays(7L)).toEpochMilli());
        ParamSpec<Timestamp> requirement = ParamSpec.builder(Timestamp.class)
                .setTable("users").setColumn("active_ts")
                .setMatcher(Matchers.newMinMaxAcceptor(min, max)).build();
        assertTrue(requirement.isAcceptableValue(min));
        assertFalse(requirement.isAcceptableValue(max));
        assertTrue(requirement.isAcceptableValue(new Timestamp(when.plus(Duration.ofDays(2)).toEpochMilli())));
        assertFalse(requirement.isAcceptableValue(new Timestamp(when.minus(Duration.ofDays(4)).toEpochMilli())));
    }

    @Test
    public void shouldConstructRequirementWithNoRestrictions() {
        ParamSpec<BigDecimal> requirement = ParamSpec.builder(BigDecimal.class)
                .setTable("users").setColumn("employee_id").build();
        assertTrue(requirement.isAcceptableValue(new BigDecimal("234879892")));
        assertTrue(requirement.isAcceptableValue(new BigDecimal("18.44")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionOnUnsupportedType() {
        ParamSpec<DayOfWeek> requirement = ParamSpec.builder(DayOfWeek.class)
                .setTable("users").setColumn("best_day").build();
        assertTrue(requirement.isAcceptableValue(DayOfWeek.FRIDAY));
    }

    @Test
    public void shouldConstructRequirementWithRegexAcceptor() {
        ParamSpec<String> requirement = ParamSpec.builder(String.class)
                .setSchema("public").setTable("users").addWhere("usertype", "poweruser").setColumn("username")
                .setMatcher(Matchers.newRegexStringAcceptor(Pattern.compile("^[ABCabc].+"))).build();
        assertTrue(requirement.isAcceptableValue("antman"));
        assertFalse(requirement.isAcceptableValue("scarletwidow"));
    }

    @Test
    public void shouldUtilizeCustomAcceptorIfProvided() {
        ParamSpec<Integer> requirement = ParamSpec.builder(Integer.class)
                .setTable("users").setColumn("ranking").setMatcher(v -> v.compareTo(8) < 0).build();
        assertTrue(requirement.isAcceptableValue(4));
        assertFalse(requirement.isAcceptableValue(16));
    }

}
