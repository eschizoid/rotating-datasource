# orm-examples

Runnable ORM integrations showing how to wire `RotatingDataSource` into popular Java ORM stacks and how to wrap ORM
calls with `Retry.authRetry` to refresh credentials and retry once on authentication failures.

## Submodules

- hibernate-app — Hibernate ORM + HikariCP
- jpa-app — JPA (Hibernate provider) + HikariCP
- jooq-app — jOOQ + HikariCP
- spring-data-app — Spring Data JPA (Spring Boot) + HikariCP

## What these examples demonstrate

- Building a `DataSourceFactory` that produces a HikariCP `DataSource` from a `DbSecret`.
- Creating a `RotatingDataSource` with your secret id, optionally with a proactive refresh interval.
- Using `Retry.authRetry` around ORM work so that when an auth-related `SQLException` occurs, the pool is reset and the
  operation retried once.

## Requirements

- Java 17+
- Maven 3.9+

## Configuration

All examples use the core module’s `SecretsManagerProvider` configuration. See the canonical list and details in:

- [sm-core/README.md](../sm-core/README.md)

At minimum, provide:

- `-Ddb.secretId=your/secret/id` (or set an environment/property the example expects)
- Optional: `-Daws.region`, `-Daws.sm.endpoint` (for Localstack), `-Daws.accessKeyId`, `-Daws.secretAccessKey`

## Build

From the repository root:

- mvn -q -pl orm-examples -am -DskipTests clean package — build all ORM examples

## Run

All submodules are configured with an executable `Main` class. You must provide a valid secret and reachable database;
the examples only perform a simple query (e.g., `select now()` or `select 1`).

**Hibernate**:

- `cd orm-examples/hibernate-app`
- `mvn -q -DskipTests clean package`
- `java -jar target/hibernate-app-1.0.0-SNAPSHOT.jar -Ddb.secretId=your/secret/id`
- `Or via Maven: mvn -q exec:java -Ddb.secretId=your/secret/id`

**JPA**:

- `cd orm-examples/jpa-app`
- `mvn -q -DskipTests clean package`
- `java -jar target/jpa-app-1.0.0-SNAPSHOT.jar -Ddb.secretId=your/secret/id`
- `Or via Maven: mvn -q exec:java -Ddb.secretId=your/secret/id`

**jOOQ**:

- `cd orm-examples/jooq-app`
- `mvn -q -DskipTests clean package`
- `java -jar target/jooq-app-1.0.0-SNAPSHOT.jar -Ddb.secretId=your/secret/id`
- Or via Maven: `mvn -q exec:java -Ddb.secretId=your/secret/id`

**Spring Data (Spring Boot)**:

- `cd orm-examples/spring-data-app`
- `mvn -q -DskipTests clean package`
- `java -jar target/spring-data-app-1.0.0-SNAPSHOT.jar -Ddb.secretId=your/secret/id`
- Or via Maven: `mvn -q spring-boot:run -Ddb.secretId=your/secret/id`

## Notes

- These demos focus on wiring. They don’t create schemas or entities.
- `Retry.authRetry` is appropriate for higher layers where exceptions are typically wrapped in `RuntimeException`; for
  lower-level JDBC code prefer `Retry.onException` or use `DbClient`.

## See also

- Core library: [sm-core/README.md](../sm-core/README.md)
- Connection pool examples: [connection-pool-examples/README.md](../connection-pool-examples/README.md)
- Project overview: [README.md](../README.md)
