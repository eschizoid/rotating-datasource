# Rotating Data Source—Connection Pool Examples

Sample runnable apps that demonstrate using sm-core with popular JDBC connection pools. Each submodule is a minimal app
with tests that show automatic recovery from AWS Secrets Manager password rotation.

Submodules:

- `c3p0-app` — uses C3P0
- `dbcp2-app` — uses Apache Commons DBCP2
- `hikaricp-app` — uses HikariCP
  - `tomcat-jdbc-app` — uses Apache Tomcat JDBC pool

## Requirements

- Java 17+
- Maven 3.9+
- Docker (required to run integration tests via Testcontainers)

## Build all examples

From the repository root:

- `mvn -q -pl connection-pool-examples -am -DskipTests clean package` — build jars without tests

Artifacts are produced under each submodule’s target/ directory.

## Run tests

Integration tests use Testcontainers to orchestrate PostgreSQL and Localstack and validate rotation behavior. See the
root README Testing section for an overview.

Commands:

- `mvn -q -pl connection-pool-examples -am clean test` — run tests (requires Docker)
- `mvn -q -Dtests.integration.disable=true -pl connection-pool-examples -am clean test` — skip integration tests

## Run an example app

Each submodule has a tiny `App` class under `src/main/java/com/example/App.java`. You must provide a valid AWS Secrets
Manager secret id and ensure the secret JSON matches the `DbSecret` fields.

Example (HikariCP):

- cd connection-pool-examples/hikaricp-app
- mvn -q -DskipTests clean package
- java -jar target/hikaricp-app-1.0.0-SNAPSHOT.jar

For environment configuration details (region, endpoint, credentials), see sm-core/README.md.

## Structure

- hikaricp-app/
    - src/main/java/com/example/App.java — demo entry point
    - src/main/java/com/example/Pool.java — builds a HikariDataSource from DbSecret
    - src/test/java/com/example/* — integration tests using Testcontainers
- tomcat-jdbc-app/
    - App.java, Pool.java, tests — the same pattern, using Tomcat JDBC pool
- dbcp2-app/
    - App.java, Pool.java, tests — the same pattern, using Apache DBCP2

## See also

- Core library: [sm-core/README.md](../rotating-data-source-core/README.md)
- ORM examples: [orm-examples/README.md](../rotating-data-source-orm-examples/README.md)
- Project overview: [README.md](../README.md)
