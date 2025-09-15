# sm-rotator

An example Java 17 project that demonstrates how to build a JDBC DataSource that automatically recovers from AWS Secrets
Manager password rotations.

It provides two small, focused building blocks:

- `RotatingDataSource` — wraps any `JDBC` `DataSource` and recreates it from an AWS Secrets Manager secret on demand.
- `DbClient` — executes `JDBC` operations and retries once on authentication failures, triggering a rotation in between.

The code is deliberately compact, uses a functional style, and is fully covered by an integration test that runs
PostgreSQL and Localstack via Testcontainers.

## Table of contents

- Features
- Architecture overview
- Modules
- Repository structure
- Requirements
- Build
- Run the examples
- Configuration
- How rotation works
- Testing (with Testcontainers)
- Logging
- Troubleshooting
- FAQ

## Features

- AWS Secrets Manager integration using AWS SDK v2
- Functional, immutable data model (DbSecret is a Java record)
- Thread-safe rotation with AtomicReference (no explicit locks)
- Optional proactive refresh via periodic secret version checks
- Optional in-memory caching of secret value/version (disabled by default)
- Declarative retry utility (`Retry.onException`)
- HikariCP example for connection pooling
- End-to-end integration test using Testcontainers (PostgreSQL + Localstack)

## Architecture overview

Core classes:

- `DbSecret` — immutable record mapping the common RDS secret JSON fields: username, password, engine, host, port,
  dbname.
- `SecretsManagerProvider` — lazily builds a Secrets Manager client; can be redirected to Localstack via config.
- `SecretHelper` — fetches a secret by id and deserializes JSON into DbSecret using Jackson.
- `DataSourceFactory` — functional interface to create a DataSource from a DbSecret (e.g., configure HikariCP).
- `RotatingDataSource` — holds a DataSource in an AtomicReference; reset() rebuilds it from the latest secret. Closes
  the previous pool if AutoCloseable.
- `Retry` — small utility for retrying a single time when a predicate on an exception matches.
- `DbClient` — runs `JDBC` operations with a single auth-retry path. If `SQLState` `28000` or certain messages are seen,
  it calls `reset()` and retries once.

## Modules

- `sm-core` — core library with `RotatingDataSource`, `DbClient`, and supporting classes.
  See [README.md](sm-core/README.md) for details.
- `connection-pool-examples` — runnable sample apps for different JDBC pools (HikariCP, Tomcat JDBC, DBCP2). See
  [README.md](connection-pool-examples/README.md).
- `orm-examples` — runnable ORM integrations (Hibernate, JPA, jOOQ, Spring Data). See
  [README.md](orm-examples/README.md).

## Repository structure

- sm-core/ — core library (`RotatingDataSource`, `DbClient`, `DbSecret`, etc.)
- connection-pool-examples/ — runnable apps showcasing different pools
    - hikaricp-app/
    - tomcat-jdbc-app/
    - dbcp2-app/
- orm-examples/ — runnable ORM integrations
    - hibernate-app/
    - jpa-app/
    - jooq-app/
    - spring-data-app/

## Requirements

- Java 17+
- Maven 3.9+
- Docker (only required for the integration test with Testcontainers)

## Build

Common tasks:

- `mvn -q -DskipTests clean package` — build all modules without tests
- `mvn -q clean test` — build all and run tests (integration tests live in connection-pool-examples; requires Docker)
- Build only sm-core: `mvn -q -pl sm-core -am -DskipTests clean package`
- Build only examples: `mvn -q -pl connection-pool-examples -am -DskipTests clean package`

## Run the examples

This repository provides runnable demo apps under `connection-pool-examples` and ORM demos under `orm-examples`. Pick a
pool or ORM and follow the instructions in their respective READMEs:

- Connection pools: [README.md](connection-pool-examples/README.md)
- ORMs: [README.md](orm-examples/README.md)

For a code-level usage example with `RotatingDataSource` and `DbClient`, see [README.md](sm-core/README.md).

## Configuration

Configuration options (region, endpoint, credentials) are documented in [README.md](sm-core/README.md). Refer
there for the canonical list and examples.

## How rotation works

1) You run a query using `DbClient`. It gets a connection from `RotatingDataSource` and executes your operation.
2) If an `SQLException` occurs with `SQLState` 28000 (or messages like "access denied" / "authentication failed"),
   `DbClient`
   treats it as an auth failure.
3) `DbClient` triggers `RotatingDataSource.reset()`:
    - `SecretHelper` loads the latest secret JSON from Secrets Manager via `SecretsManagerProvider`.
    - `DataSourceFactory` builds a new `DataSource` (e.g., a new Hikari pool) from the updated secret.
    - The previous pool (if `AutoCloseable`) is closed.
4) `DbClient` retries the operation once using the freshly rebuilt `DataSource`.

The logic is straightforward by design but resilient to password rotation as long as your secret is updated.

## Testing (with Testcontainers)

Integration tests are located under `connection-pool-examples` and use Testcontainers to run PostgreSQL and Localstack.
See [README.md](connection-pool-examples/README.md) for details.

Commands (from repository root):

- `mvn -q -pl connection-pool-examples -am clean test` — run example integration tests (requires Docker)
- `mvn -q -Dtests.integration.disable=true -pl connection-pool-examples -am clean test` — skip integration tests

## Logging

This project uses Java's built-in System.Logger for lightweight logging. No extra logging dependency is required.
You can control logging using standard JDK logging configuration (`java.util.loggin`g), for example, by providing
`-Djava.util.logging.config.file` to point at a properties file.

## Troubleshooting

- Tests hang or are very slow when Docker is unavailable:
    - The integration test contains a fast-fail check and will be skipped when Docker is not accessible. You can also
      force-skip with -Dtests.integration.disable=true.
- Authentication errors persist after rotation:
    - Verify your secret in AWS matches DbSecret fields and that your DataSourceFactory uses the same
      engine/host/port/dbname/username.
    - Ensure SecretsManagerProvider is pointed at the correct region/endpoint.
- SLF4J warnings about missing StaticLoggerBinder:
    - The project depends on slf4j-simple; ensure your classpath is clean if you swap logging implementations.

## FAQ

**Q: Does RotatingDataSource rotate proactively?**
A: It can. By default, it refreshes on demand when you call reset() (triggered by DbClient on auth failures). You can
also enable proactive refresh by using the RotatingDataSource(secretId, factory, refreshIntervalSeconds) constructor
with a positive interval; it will periodically check the secret version and refresh when it changes.

**Q: Is there connection leakage during reset?**
A: `RotatingDataSource` closes the previous DataSource if it implements `AutoCloseable` (`HikariDataSource` does).
Existing connections remain valid until closed by your code or the pool.

**Q: Can I customize the retry policy?**
A: The Retry utility is intentionally minimal. You can copy it and extend it (e.g., backoff, max attempts, exception
classifier) without changing the rest of the code.

**Q: Does this require a specific pool?**
A: No. Any `DataSource` can be created by your `DataSourceFactory`. `HikariCP` is used in examples and tests.

**Q: Can I use DbClient with the ORM examples (Hibernate/JPA/Spring Data)?**
A: Yes, but it depends on your goal. `DbClient` is a thin `JDBC` helper with an auth-failure retry. ORMs already manage
units of work and sessions; they don’t automatically use `DbClient`. The recommended approach is to configure your ORM
to
use RotatingDataSource as its DataSource so credential rotation is handled transparently. Use `DbClient` only for any
ad‑hoc raw `JDBC` operations you perform alongside the ORM. If you want a retry on authentication errors around ORM
operations themselves, implement that at the service layer (e.g., catch the auth error, call RotatingDataSource.reset(),
and retry once). No ORM-specific changes are required to benefit from rotation.

**Q: Are Retry.authRetry and Retry.onException both needed?**
A: Yes, they address two different layers and exception models:

- `Retry.onException` is a generic utility for operations that throw checked `SQLException`s. DbClient uses it
  internally
  to implement the single auth-failure retry with a RotatingDataSource.reset() in between. Use onException when you are
  writing lower-level `JDBC` code that deals with checked exceptions and you want a predicate-driven retry with optional
  delays/attempts.
- `Retry.authRetry `is a convenience for higher layers (ORMs/service methods) where failures are often wrapped in
  RuntimeExceptions. It inspects the exception cause chain for an authentication-related SQLException, resets the
  RotatingDataSource, and retries once. Use authRetry when integrating with Hibernate/JPA/jOOQ/Spring Data calls as
  shown in the orm-examples.
  If you don’t need the higher-level convenience (for example, you only use `DbClient` or your own retry wrapper), you
  can ignore `authRetry`. Conversely, if you only need the ORM convenience, you can ignore `onException`. They are
  small, independent helpers.
