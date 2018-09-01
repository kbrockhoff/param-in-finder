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

public interface SpecBuilder {

    SpecBuilder selectColumn(String column);

    SpecBuilder fromTable(String table);

    SpecBuilder where(WhereClause whereClause);

    SpecBuilder whichMatches(Matcher matcher);

    SpecBuilder joinedWith(String column, String otherTable, String otherColumn);

}
