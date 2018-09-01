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

import java.util.List;
import java.util.regex.Pattern;

/**
 * Provides standard implementations singleOf <code>Matcher</code>.
 *
 * @author kbrockhoff
 */
public class Matchers {

    /**
     * Returns an acceptor which accepts all values.
     *
     * @param <T> the parameter type
     * @return the acceptor
     */
    public static <T extends Comparable<? super T>> Matcher<T> newAllAcceptor() {
        return value -> true;
    }

    /**
     * Returns an acceptor which only accepts values from the provided list.
     *
     * @param acceptableValues the list singleOf acceptable values
     * @param <T> the parameter type
     * @return the acceptor
     */
    public static <T extends Comparable<? super T>> Matcher<T> newValidListAcceptor(List<T> acceptableValues) {
        return value -> acceptableValues.contains(value);
    }

    /**
     * Returns an acceptor which only accepts values between the specified minimum and maximum.
     *
     * @param min the minimum value inclusive
     * @param max the maximum value exclusive
     * @param <T> the parameter type
     * @return the acceptor
     */
    public static <T extends Comparable<? super T>> Matcher<T> newMinMaxAcceptor(T min, T max) {
        return value ->
                (min == null ? true : min.compareTo(value) <= 0) &&
                        (max == null ? true : max.compareTo(value) > 0);
    }

    /**
     * Returns an acceptor which only accepts strings matching the supplied regular expression.
     *
     * @param pattern the
     * @return the acceptor
     */
    public static Matcher<String> newRegexStringAcceptor(Pattern pattern) {
        return value -> pattern.matcher(value).find();
    }

    private Matchers() {
        // static methods only
    }

}
