# Test Case In Parameter Finder

This compact Java library eases development of non-flaky integration and
performance tests. It provides an alternative to complex test data setup and
teardown routines. Using JDBC, it retrieves input parameters from data already
existing in the database under test based on a set of specifications or
requirements. It facilitates filtering out junk values which often accumulate
in development and test databases. It can be particularly useful for
performance testing of large databases because a large set of different input
parameters representative of actual load patterns can be retrieved
during test setup.

It requires at least Java 8. It uses a reactive streams design but uses
a LinkedIn-source library which backports the Java 9 Flow API into its
own package to avoid clashes with the JDK equivalents. It is expected this
library will switch the JDK-provided API once the majority of development
organizations have migrated to Java 9 or higher. The only other dependency is
the SLF4J API and the `javax.inject` package so the library can be introduced
without introducing classpath conflicts.

The main class is `FindParametersExecutor` which is designed to be managed as
a singleton by any of the popular dependency injection frameworks. The class is
has `javax.inject` package annotations so classpath scanning can be used for
dependency wiring. It is also easy to wire up the executor in test setup
without the aid of a DI framework.

## Simple Example

```java
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

    @Before
    public void setUp() throws SQLException, IOException {
        // instantation of data source not shown
        findParametersExecutor = new FindParametersExecutor(dataSource);
    }

    @Test
    public void testOfSomeFunctionality() {
        ParamSpecs paramSpecs = create(find(String.class).fromTable("types").inColumn("name").build())
                .join(find(String.class).fromTable("pets").inColumn("id").build(), new JoinPair("id", "type_id"))
                .join(find(String.class).fromTable("owners").inColumn("city").build(), new JoinPair("owner_id", "id"));
        int size = 4;
        Future<Set<Tuple>> future = findParametersExecutor.findValidParameters(paramSpecs, size);
        Set<Tuple> paramTuples = future.get();
        paramTuples.forEach(t -> testGetOrQueryMethod(t));
    }
    
    private void testGetOrQueryMethod(Tuple parameters) {
        // actual test not shown
    }

}
```

## Filtering Returned Values

There are two options for filtering which values are returned. The first is
is adding where conditions to a specification which will be added to the SQL
WHERE clause when querying for data.

```java
    ParamSpec<String> spec = ParamSpec.find(String.class)
            .inColumn("username").fromTable("users")
            .where(new Condition("usertype", Operator.EQUALS, "poweruser"))
            .where(new Condition("status", Operator.EQUALS, "ACTIVE"))
            .build();

```

The second is supplying a matcher other than the default all values matcher
to a parameter specification. The included `Matchers` class provides
instances of commonly used matchers or you can write your own.

```java
    ParamSpec<String> spec = ParamSpec.find(String.class)
            .fromTable("public", "users").inColumn("usertype")
            .matching(Matchers.newValidListAcceptor(Arrays.asList("administrator", "poweruser")))
            .build();

```
