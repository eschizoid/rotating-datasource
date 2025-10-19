# Rotating Data Source—Core

Core library that implements a rotating JDBC DataSource powered by AWS Secrets Manager with automatic credential
rotation handling. This module provides reusable building blocks for applications that need to handle database
credential rotations seamlessly.

## What's inside

- **`DbSecret`** — immutable record mapping AWS RDS-like secret JSON (username, password, engine, host, port, dbname)
- **`SecretsManagerProvider`** — builds/configures the AWS Secrets Manager client
- **`SecretHelper`** — loads and parses a secret into `DbSecret`
- **`DataSourceFactory`** — functional interface to build a `javax.sql.DataSource` from a `DbSecret`
- **`RotatingDataSource`** — DataSource wrapper with automatic retry on authentication failures and optional proactive
  secret refresh
- **`Retry`** — flexible retry mechanism with configurable policies (fixed delay, exponential backoff) and
  vendor-specific auth error detection

## Features

### Automatic Credential Rotation Handling

- **Transparent retry** — `RotatingDataSource.getConnection()` automatically retries on auth failures
- **Configurable retry policies** — fixed delay or exponential backoff with jitter
- **Vendor-specific error detection** — PostgreSQL and MySQL out-of-the-box
- **Proactive refresh** — optional periodic secret version checking and DataSource refresh
- **Thread-safe** — safe concurrent access from multiple threads

### Integration Support

- Works with any JDBC connection pool (HikariCP, Tomcat JDBC, DBCP2, etc.)
- Spring Boot / Spring Framework compatible
- Hibernate / JPA compatible
- Plain JDBC compatible

## Requirements

- Java 17+
- Maven 3.9+
- AWS credentials configured (via environment, IAM role, or AWS CLI)
- AWS Secrets Manager secret containing database credentials

## Build this module

From the repository root:

```bash
# Build only rotating-datasource-core
mvn -q -pl rotating-datasource-core -am -DskipTests clean package

# Run tests
mvn -q -pl rotating-datasource-core test

# Artifact location
# rotating-datasource-core/target/rotating-datasource-core-1.0.0-SNAPSHOT.jar
```

## Quick Start

### Basic Usage with HikariCP

```java
import com.example.rotating.datasource.core.jdbc.core.DataSourceFactoryProvider;
import com.example.rotating.datasource.core.jdbc.core.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

final var factory = secret -> {
    var cfg = new HikariConfig();
    cfg.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
    cfg.setUsername(secret.username());
    cfg.setPassword(secret.password());
    cfg.setMaximumPoolSize(10);
    return new HikariDataSource(cfg);
};

final var rotatingDs = RotatingDataSource.builder()
    .secretId("my-db-secret")
    .factory(factory)
    .refreshIntervalSeconds(60)
    .build();

try (final var conn = rotatingDs.getConnection(); final var stmt = conn.createStatement()) {
  final var rs = stmt.executeQuery("SELECT current_user");
  rs.next();
}
```

### With Proactive Secret Refresh

```java
// Check the secret version every 60 seconds and refresh if changed
final var rotatingDs = RotatingDataSource.builder()
    .secretId("my-db-secret")
    .factory(factory)
    .refreshIntervalSeconds(60)  // refresh interval in seconds
    .build();

// Remember to shut down when done
Runtime.getRuntime().addShutdownHook(new Thread(rotatingDs::shutdown));
```

### Custom Retry Policy

```java
// Exponential backoff: 5 attempts, starting at 100ms, max 30s
final var policy = Retry.Policy.exponential(5, 100L);

final var rotatingDs = RotatingDataSource.builder()
    .secretId("my-db-secret")
    .factory(factory)
    .retryPolicy(policy)   // custom retry policy
    .build();
```

### Vendor-Specific Error Detection

```java
// Oracle-specific authentication error codes
final var oracleDetector = Retry.AuthErrorDetector.custom(e ->
    e.getErrorCode() == 1017 ||  // ORA-01017: invalid username/password
    e.getErrorCode() == 28000    // ORA-28000: account is locked
);

final var policy = Retry.Policy.fixed(3, 100L);

final var rotatingDs = RotatingDataSource.builder()
    .secretId("oracle-secret")
    .factory(secret -> createOracleDataSource(secret))
    .retryPolicy(policy)
    .authErrorDetector(oracleDetector)
    .build();
```

## Advanced Usage

- For lower-level JDBC code paths that need per-operation retries, prefer `Retry.onException(...)` with a
  `Retry.Policy`.
- ORMs usually do not need extra wrappers; optionally use `Retry.authRetry(...)` for long-running units of work that may
  straddle a rotation and surface an auth-related SQLException.

### Spring Boot Integration

```java

@Configuration
public class DataSourceConfig {

    @Bean
    public RotatingDataSource rotatingDataSource() {
        final var factory = secret -> {
            final var config = new HikariConfig();
            config.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
            config.setUsername(secret.username());
            config.setPassword(secret.password());
            config.setMaximumPoolSize(20);
            config.setMinimumIdle(5);
            config.setIdleTimeout(300000);
            return new HikariDataSource(config);
        };

        return new RotatingDataSource(
            "my-db-secret",
            factory,
            60L,  // proactive refresh every 60 seconds
            Retry.Policy.exponential(5, 200L)
        );
    }

    @Bean
    public DataSource dataSource(RotatingDataSource rotatingDataSource) {
        return rotatingDataSource;
    }

    @PreDestroy
    public void cleanup(RotatingDataSource rotatingDataSource) {
        rotatingDataSource.shutdown();
    }
}
```

### Hibernate/JPA Integration

```java
// Configure Hibernate to use RotatingDataSource
final var rotatingDs = RotatingDataSource.builder()
    .secretId("my-db-secret")
    .factory(factory)
    .refreshIntervalSeconds(60L)
    .build();

final var sessionFactory = new Configuration()
    .setProperty("hibernate.connection.provider_class", "org.hibernate.connection.DatasourceConnectionProvider")
    .setProperty("hibernate.connection.datasource", rotatingDs)
    .buildSessionFactory();

// Use sessions normally - connection acquisition automatically retries
try (var session = sessionFactory.openSession()) {
    var users = session.createQuery("FROM User WHERE active = true", User.class).list();
}

// For operations that may encounter auth errors after connection establishment
final var users = Retry.authRetry(
    () -> {
        try (var session = sessionFactory.openSession()) {
            return session.createQuery("FROM User", User.class).list();
        }
    },
    rotatingDs
);
```

### Manual Reset on Detected Rotation

```java
try (var conn = rotatingDs.getConnection()) {
    // execute queries
} catch (SQLException e) {
    if (Retry.isAuthError(e)) {
        System.out.println("Auth error detected, manually resetting credentials...");
        rotatingDs.reset();
        // retry operation
    }
    throw e;
}
```

### Read Replica Configuration

```java
// Primary database
final var primaryDs = RotatingDataSource.builder()
    .secretId("primary-db-secret")
    .factory(secret -> createHikariDataSource(secret, false))
    .refreshIntervalSeconds(60L)
    .build();

// Read replica (read-only)
final var replicaDs = RotatingDataSource.builder()
    .secretId("replica-db-secret")
    .factory(secret -> {
        var config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
        config.setUsername(secret.username());
        config.setPassword(secret.password());
        config.setReadOnly(true);
        config.setMaximumPoolSize(20);
        return new HikariDataSource(config);
    })
    .refreshIntervalSeconds(60L)
    .build();
```

## How It Works

### Credential Rotation Flow

1. **Normal operation**: `getConnection()` returns connections from the current pool
2. **Credential rotation occurs** in AWS Secrets Manager
3. **Auth failure detected**: Application attempts connection with old credentials
4. **Automatic recovery**:
    - `RotatingDataSource` catches authentication exception
    - Fetches latest secret from AWS Secrets Manager
    - Closes old connection pool (if `AutoCloseable`)
    - Creates new pool with fresh credentials
    - Retries connection acquisition
5. **Success**: Application continues with new credentials

### Proactive Refresh (Optional)

When enabled (`refreshIntervalSeconds > 0`):

- Background scheduler periodically checks secret version
- If version changes, proactively refreshes DataSource
- Minimizes window where stale credentials might be used
- No retry needed since credentials are already fresh

## Error Handling

### Built-in Authentication Error Detection

The library includes detection for common authentication errors:

- **PostgreSQL**: `28P01`, `28000`
- **MySQL**: `1045`, `1044`, `1862`

### Custom Error Detection

```java
// Add custom error codes
final var customDetector = Retry.AuthErrorDetector.custom(e ->
    e.getErrorCode() == 12345 ||
    e.getMessage().contains("authentication failed")
);

// Combine with default detector
final var combined = Retry.AuthErrorDetector.defaultDetector().or(customDetector);
```

## Testing

The library includes comprehensive tests using Testcontainers for PostgreSQL and MySQL:

```bash
# Run all tests in core module
mvn -q -pl rotating-datasource-core test

# Run specific test
mvn -q -pl rotating-datasource-core test -Dtest=RotatingDataSourceTest

# Run integration tests only
mvn -q -pl rotating-datasource-core test -Dtest=*IntegrationTest
```

## Thread Safety

- `RotatingDataSource` is fully thread-safe
- Multiple threads can safely call `getConnection()` and `reset()` concurrently
- DataSource reference is atomically swapped during refresh
- No connection pool reconfiguration — always create fresh instance

## Resource Management

Always call `shutdown()` when done:

```java
final var rotatingDs = new RotatingDataSource("my-db-secret", factory, 60L);

try {
    // use rotatingDs
} finally {
    rotatingDs.shutdown();  // stops scheduler, closes pool
}
```

## Configuration Best Practices

1. **Use proactive refresh** for long-running applications (60-300 seconds)
2. **Use exponential backoff** for production (5 attempts, 100-200ms initial delay)
3. **Configure connection pool properly** (max pool size, timeouts, health checks)
4. **Monitor authentication failures** to detect rotation issues early
5. **Test rotation** in staging with actual secret rotation

## RDS + Secrets Manager Integration (PostgreSQL and MySQL)

This library is designed to work out‑of‑the‑box with AWS RDS when database credentials are rotated via AWS Secrets
Manager.

- Secrets Manager stores a JSON document with fields similar to the following.
- RotatingDataSource reads that secret using SecretsManagerProvider/SecretHelper and builds your preferred connection
  pool via a DataSourceFactory.
- When Secrets Manager rotates the password, RotatingDataSource detects it and swaps in a new pool without downtime.

### Secret JSON formats

PostgreSQL example:

```json
{
  "username": "app_user",
  "password": "s3cr3t",
  "engine": "postgres",
  "host": "example.cluster-abcdefghijkl.us-east-1.rds.amazonaws.com",
  "port": 5432,
  "dbname": "appdb"
}
```

MySQL example:

```json
{
  "username": "app_user",
  "password": "s3cr3t",
  "engine": "mysql",
  "host": "example.cluster-abcdefghijkl.us-east-1.rds.amazonaws.com",
  "port": 3306,
  "dbname": "appdb"
}
```

### Minimal configuration (HikariCP)

PostgreSQL

```java
final var ds = RotatingDataSource.builder()
        .secretId("rds/postgres/app")
        .factory(secret -> {
            var cfg = new HikariConfig();
            cfg.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
            cfg.setUsername(secret.username());
            cfg.setPassword(secret.password());
            cfg.setMaximumPoolSize(10);
            return new HikariDataSource(cfg);
        })
        // Postgres typically does NOT support dual valid passwords
        .overlapDuration(Duration.ZERO)      // disable overlap fallback
        .gracePeriod(Duration.ofSeconds(60)) // let in-flight sessions drain
        .refreshIntervalSeconds(60)          // optional proactive refresh
        .build();
```

MySQL/Aurora MySQL (with dual-password overlap in rotation procedure)

```java
final var ds = RotatingDataSource.builder()
        .secretId("rds/mysql/app")
        .factory(secret -> {
            var cfg = new HikariConfig();
            cfg.setJdbcUrl("jdbc:mysql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
            cfg.setUsername(secret.username());
            cfg.setPassword(secret.password());
            cfg.setMaximumPoolSize(10);
            return new HikariDataSource(cfg);
        })
        .overlapDuration(Duration.ofHours(24)) // fallback to old pool on auth failures during overlap
        .gracePeriod(Duration.ofSeconds(60))   // used only when overlap is zero
        .refreshIntervalSeconds(60)            // optional proactive refresh
        .build();
```

Notes:

- If your MySQL rotation does NOT retain the old password concurrently, set overlapDuration(Duration.ZERO) like
  PostgreSQL.
- For Aurora/MySQL with retain-current-password or custom rotation Lambdas that keep both passwords valid, a non-zero
  overlapDuration provides seamless fallback for new connections while the new credentials propagate.

### How rotation is detected and handled

- Proactive: if refreshIntervalSeconds > 0, a background task periodically compares the latest secret versionId; if it
  changed, reset() swaps to a new pool built from the updated secret.
- Reactive: if getConnection() encounters an authentication failure, Retry.authRetry triggers reset() and retries once.
- Overlap window (if configured): during overlapDuration, RotatingDataSource keeps the old pool as a secondary. If the
  new primary fails auth, getConnection() falls back to the secondary, avoiding error spikes.
- Grace period (when overlap is zero): the old pool stays open for gracePeriod so already-checked-out connections finish
  cleanly while new connections use the new pool.

### Builder properties explained

- secretId: Secrets Manager secret name/ARN that stores your DB credentials.
- factory: function that builds your connection pool (HikariCP, Tomcat JDBC, DBCP2, etc.) using the current secret.
- refreshIntervalSeconds: proactive check interval (0 disables). Helps shorten the window between rotation and client
  refresh.
- retryPolicy: transient retry policy (fixed/exponential). Applied around connection acquisition, with jitter to avoid
  thundering herd.
- authErrorDetector: pluggable detector for vendor-specific authentication errors (defaults cover common cases for
  Postgres/MySQL; extend for Oracle, etc.).
- overlapDuration: enable dual-password overlap fallback for engines/procedures that keep old and new passwords valid
  concurrently (typical for some MySQL/Aurora flows). Default: 15 minutes.
- gracePeriod: drain window used when overlapDuration is zero. Recommended for PostgreSQL. Default: 60 seconds.

### Environment and local testing

- Configure AWS region/endpoint/credentials via system properties or environment variables; see
  rotating-datasource-core/SecretsManagerProvider for details.
- Integration tests in rotating-datasource-orm-examples use Localstack to emulate Secrets Manager and Testcontainers for
  PostgreSQL.

### JDBC vs ORMs

- JDBC: Inject/use the RotatingDataSource like any DataSource. No extra retry code is required; connection acquisition
  retries automatically. For simple queries you can use JdbcTemplate or plain JDBC normally.
- ORMs (Hibernate/JPA/Spring Data/jOOQ): Wire RotatingDataSource as the DataSource. ORMs inherit retry on connection
  acquisition. Use Retry.authRetry only for rare long-running units of work that straddle a password rotation.

## Related

- See [connection-pool-examples/README.md](../rotating-data-source-connection-pool-examples/README.md) for complete
  runnable examples with HikariCP
- See [README.md](../README.md) for overall project documentation
