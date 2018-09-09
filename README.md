# Test Case In Parameter Finder

This compact Java library eases development of non-flaky integration and
performance tests. It provides an alternative to complex test data setup and
teardown routines. Using JDBC, it retrieves input parameters from data already
existing in the database under test. Data is retrieved based on a set of
specifications (i.e. requirements). It facilitates filtering out junk values
which often accumulate in development and test databases. It can be
particularly useful for performance testing of large databases because a large
set of different input parameters representative of actual load patterns can
be retrieved during test setup.

It requires at least Java 8. It uses a reactive streams design but uses
a LinkedIn-sourced library which backports the Java 9 Flow API into its
own package thereby avoiding clashes with the JDK equivalents. This library
will switch to the JDK-provided API once the majority of development
organizations migrate to Java 9 or higher. The only other dependencies are
the SLF4J API and the `javax.inject` package. Therefore the library can be
introduced without introducing classpath conflicts.

The main class, `FindParametersExecutor`, is designed to be managed as
a singleton by any of the popular dependency injection frameworks. The class
has `javax.inject` package annotations so classpath scanning can be used for
dependency wiring. It is also easy to wire up the executor in test suite setup
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
is adding where conditions to a specification which will be included in the
generate SQL WHERE clause when querying for data.

```java
    ParamSpec<String> spec = ParamSpec.find(String.class)
            .inColumn("username").fromTable("users")
            .where(new Condition("usertype", Operator.EQUALS, "poweruser"))
            .where(new Condition("status", Operator.EQUALS, "ACTIVE"))
            .build();

```

The second is by supplying a matcher other than the default all values matcher
to a parameter specification. The included `Matchers` class provides
instances of commonly used matchers or you can write your own.

```java
    ParamSpec<String> spec = ParamSpec.find(String.class)
            .fromTable("public", "users").inColumn("usertype")
            .matching(Matchers.newValidListAcceptor(Arrays.asList("administrator", "poweruser")))
            .build();

```

## Supported Datatypes

Currently only a subset of JDBC types are supported. They include the most
common types of columns which are typically referenced in WHERE clauses.
The supported types are:

| Java type            | JDBCType    |
| -------------------- | ----------- |
| java.lang.String     | VARCHAR     |
| java.math.BigDecimal | DECIMAL     |
| java.lang.Integer    | INTEGER     |
| java.lang.Long       | BIGINT      |
| java.sql.Date        | DATE        |
| java.sql.Timestamp   | TIMESTAMP   |
| java.lang.Character  | CHAR        |
| java.lang.Boolean    | BOOLEAN     |

## Links

* Javadoc
