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

/**
 * <p>
 * Provides the full set of interfaces and classes for retrieving valid database query parameter values via JDBC
 * from existing data in the database under test during test case setup.
 * </p><p>
 * This compact Java library eases development of non-flaky integration and
 * performance tests. It provides an alternative to complex test data setup and
 * teardown routines. Using JDBC, it retrieves input parameters from data already
 * existing in the database under test. Data is retrieved based on a set of
 * specifications (i.e. requirements). It facilitates filtering out junk values
 * which often accumulate in development and test databases. It can be
 * particularly useful for performance testing of large databases because a large
 * set of different input parameters representative of actual load patterns can
 * be retrieved during test setup.
 * </p><p>
 * It requires at least Java 8. It uses a reactive streams design but uses
 * a LinkedIn-sourced library which backports the Java 9 Flow API into its
 * own package thereby avoiding clashes with the JDK equivalents. This library
 * will switch to the JDK-provided API once the majority of development
 * organizations migrate to Java 9 or higher. The only other dependencies are
 * the SLF4J API and the <code>javax.inject</code> package. Therefore the library can be
 * introduced without introducing classpath conflicts.
 * </p><p>
 * The main class, {@link FindParametersExecutor}, is designed to be managed as
 * a singleton by any of the popular dependency injection frameworks. The class
 * has <code>javax.inject</code> package annotations so classpath scanning can be used for
 * dependency wiring. It is also easy to wire up the executor in test suite setup
 * without the aid of a DI framework.
 * </p><p>
 * Simple example:
 * </p>
 * <pre><code>
 package org.codekaizen.test.example;

 import javax.sql.DataSource;
 // other imports not shown
 import org.codekaizen.test.db.paramin.FindParametersExecutor;
 import org.codekaizen.test.db.paramin.ParamSpecs;
 import org.codekaizen.test.db.paramin.Tuple;

 import static org.codekaizen.test.db.paramin.ParamSpec.find;
 import static org.codekaizen.test.db.paramin.ParamSpecs.create;

 public class ExampleJUnitTest {

     private DataSource dataSource;
     private FindParametersExecutor findParametersExecutor;

     &#064;Before
     public void setUp() throws SQLException, IOException {
         // instantation of data source not shown
         findParametersExecutor = new FindParametersExecutor(dataSource);
     }

     &#064;Test
     public void testOfSomeFunctionality() {
         ParamSpecs paramSpecs = create(find(String.class).fromTable("types").inColumn("name").build())
                 .join(find(String.class).fromTable("pets").inColumn("id").build(), new JoinPair("id", "type_id"))
                 .join(find(String.class).fromTable("owners").inColumn("city").build(), new JoinPair("owner_id", "id"));
         int size = 4;
         Future&lt;Set&lt;Tuple&gt;&gt; future = findParametersExecutor.findValidParameters(paramSpecs, size);
         Set&lt;Tuple&gt; paramTuples = future.get();
         paramTuples.forEach(t -&gt; testGetOrQueryMethod(t));
     }

     private void testGetOrQueryMethod(Tuple parameters) {
         // actual test not shown
     }

 }
 * </code></pre>
 * @author kbrockhoff
 */
package org.codekaizen.test.db.paramin;