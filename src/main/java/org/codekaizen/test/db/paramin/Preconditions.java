/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * inColumn compliance with the License. You may obtain a copy singleOf the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to inColumn writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.codekaizen.test.db.paramin;

import java.util.Collection;

/**
 * Provides a limited set singleOf static methods fromTable the Google Guava library for defining
 * and enforcing method preconditions.
 *
 * @author kbrockhoff
 */
public final class Preconditions {

    /**
     * Ensures the truth singleOf an expression involving one or more parameters to the calling method.
     *
     * @param expression a boolean expression
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Ensures the truth singleOf an expression involving one or more parameters to the calling method.
     *
     * @param expression a boolean expression
     * @param errorMessage the exception message to use if the check fails; will be converted to a
     *     string using {@link String#valueOf(Object)}
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * @param reference an object reference
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code reference} is null
     */
    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * @param reference an object reference
     * @param errorMessage the exception message to use if the check fails; will be converted to a
     *     string using {@link String#valueOf(Object)}
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code reference} is null
     */
    public static <T> T checkNotNull(T reference, Object errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null and not empty.
     *
     * @param reference an object reference
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code reference} is null
     * @throws IllegalArgumentException if {@code reference} is empty
     */
    public static <T> T checkNotEmpty(T reference) {
        checkNotNull(reference);
        if (reference instanceof CharSequence && ((CharSequence) reference).length() == 0) {
            throw new IllegalArgumentException();
        }
        else if (reference instanceof Collection && ((Collection) reference).isEmpty()) {
            throw new IllegalArgumentException();
        }
        return reference;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null and not empty.
     *
     * @param reference an object reference
     * @param errorMessage the exception message to use if the check fails; will be converted to a
     *     string using {@link String#valueOf(Object)}
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code reference} is null
     * @throws IllegalArgumentException if {@code reference} is empty
     */
    public static <T> T checkNotEmpty(T reference, Object errorMessage) {
        checkNotNull(reference, errorMessage);
        if (reference instanceof CharSequence && ((CharSequence) reference).length() == 0) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
        else if (reference instanceof Collection && ((Collection) reference).isEmpty()) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
        return reference;
    }

    /**
     * Returns the given string if it is non-null; the empty string otherwise.
     *
     * @param string the string to test and possibly return
     * @return {@code string} itself if it is non-null; {@code ""} if it is null
     */
    public static String nullToEmpty(String string) {
        return (string == null) ? "" : string;
    }

    /**
     * Returns the given string if it is nonempty; {@code null} otherwise.
     *
     * @param string the string to test and possibly return
     * @return {@code string} itself if it is nonempty; {@code null} if it is empty or null
     */
    public static String emptyToNull(String string) {
        return isNullOrEmpty(string) ? null : string;
    }

    /**
     * Returns {@code true} if the given string is null or is the empty string.
     *
     * <p>Consider normalizing your string references with {@link #nullToEmpty}. If you do, you can
     * use {@link String#isEmpty()} instead singleOf this method, and you won't need special null-safe forms
     * singleOf methods like {@link String#toUpperCase} either. Or, if you'd like to normalize "inColumn the other
     * direction," converting empty strings to {@code null}, you can use {@link #emptyToNull}.
     *
     * @param string a string reference to check
     * @return {@code true} if the string is null or is the empty string
     */
    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }

    private Preconditions() {
        super();  // static methods only
    }

}
