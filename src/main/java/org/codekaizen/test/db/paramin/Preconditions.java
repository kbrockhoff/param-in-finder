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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * Provides a limited set of static methods for defining and enforcing method preconditions.
 *
 * @author kbrockhoff
 */
final class Preconditions {

    /**
     * Ensures the truth of a supplied expression.
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
     * Ensures the truth of a supplied expression.
     *
     * @param expression   a boolean expression
     * @param errorMessage the exception message to use if the check fails
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    /**
     * Ensures the supplied object reference is not null.
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
     * Ensures the supplied object reference is not null.
     *
     * @param reference    an object reference
     * @param errorMessage the exception message to use if the check fails
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
     * Ensures the supplied object reference is not null and not empty.
     *
     * @param reference an object reference
     * @return the non-null reference that was validated
     * @throws NullPointerException     if {@code reference} is null
     * @throws IllegalArgumentException if {@code reference} is empty
     */
    public static <T> T checkNotEmpty(T reference) {
        checkNotNull(reference);
        if (reference instanceof CharSequence && ((CharSequence) reference).length() == 0) {
            throw new IllegalArgumentException();
        } else if (reference instanceof Collection && ((Collection) reference).isEmpty()) {
            throw new IllegalArgumentException();
        }
        return reference;
    }

    /**
     * Ensures the supplied object reference is not null and not empty.
     *
     * @param reference    an object reference
     * @param errorMessage the exception message to use if the check fails
     * @return the non-null reference that was validated
     * @throws NullPointerException     if {@code reference} is null
     * @throws IllegalArgumentException if {@code reference} is empty
     */
    public static <T> T checkNotEmpty(T reference, Object errorMessage) {
        checkNotNull(reference, errorMessage);
        if (reference instanceof CharSequence && ((CharSequence) reference).length() == 0) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        } else if (reference instanceof Collection && ((Collection) reference).isEmpty()) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        } else if (reference instanceof Connection) {
            Connection connection = (Connection) reference;
            try {
                if (connection.isClosed()) {
                    throw new IllegalArgumentException(String.valueOf(errorMessage));
                }
            } catch (SQLException cause) {
                throw new IllegalArgumentException(String.valueOf(errorMessage), cause);
            }
        } else if (reference instanceof Statement) {
            Statement statement = (Statement) reference;
            try {
                if (statement.isClosed()) {
                    throw new IllegalArgumentException(String.valueOf(errorMessage));
                }
            } catch (SQLException cause) {
                throw new IllegalArgumentException(String.valueOf(errorMessage), cause);
            }
        }
        return reference;
    }

    /**
     * Returns the given string if it is non-null or else an empty string.
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
        return isBlank(string) ? null : string;
    }

    /**
     * Returns {@code true} if the given string is null or is the empty string.
     *
     * @param str a string reference to check
     * @return {@code true} if the string is null or is an empty string
     */
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private Preconditions() {
        super();  // static methods only
    }

}
