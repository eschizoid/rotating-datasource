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
- **`DbClient`** — executes JDBC operations with automatic retry on authentication failures

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
# Build only sm-core
mvn -q -pl sm-core -am -DskipTests clean package

# Run tests
mvn -q -pl sm-core test

# Artifact location
# sm-core/target/sm-core-1.0.0-SNAPSHOT.jar
```

## Quick Start

### Basic Usage with HikariCP

```java

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

// Define how to create a DataSource from a DbSecret
final var factory = secret -> {
    var config = new HikariConfig();
    config.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
    config.setUsername(secret.username());
    config.setPassword(secret.password());
    config.setMaximumPoolSize(10);
    return new HikariDataSource(config);
};

// Create a rotating data source (default: 2 retry attempts, 50ms delay)
final var rotatingDs = new RotatingDataSource("my-db-secret", factory);

// Use like any DataSource - automatic retry on auth failures
try (var conn = rotatingDs.getConnection()) {
    try (var stmt = conn.createStatement()) {
        var rs = stmt.executeQuery("SELECT current_user");
        rs.next();
        System.out.println("Connected as: " + rs.getString(1));
    }
}
```

### With Proactive Secret Refresh

```java
// Check the secret version every 60 seconds and refresh if changed
final var rotatingDs = new RotatingDataSource(
    "my-db-secret",
    factory,
    60L  // refresh interval in seconds
);

// Remember to shut down when done
Runtime.getRuntime().addShutdownHook(new Thread(rotatingDs::shutdown));
```

### Custom Retry Policy

```java
// Exponential backoff: 5 attempts, starting at 100ms, max 30s
final var policy = Retry.Policy.exponential(5, 100L);

final var rotatingDs = new RotatingDataSource(
    "my-db-secret",
    factory,
    0L,      // no proactive refresh
    policy   // custom retry policy
);
```

### Vendor-Specific Error Detection

```java
// Oracle-specific authentication error codes
var oracleDetector = Retry.AuthErrorDetector.custom(e ->
    e.getErrorCode() == 1017 ||  // ORA-01017: invalid username/password
    e.getErrorCode() == 28000    // ORA-28000: account is locked
);

var policy = Retry.Policy.fixed(3, 100L);

var rotatingDs = new RotatingDataSource(
    "oracle-secret",
    secret -> createOracleDataSource(secret),
    0L,
    policy,
    oracleDetector
);
```

## Advanced Usage

### Using DbClient for Higher-Level Operations

```java
final var client = new DbClient(rotatingDs);

// Execute query with automatic retry
final var users = client.executeWithRetry(conn -> {
    try (var stmt = conn.prepareStatement("SELECT * FROM users WHERE active = ?")) {
        stmt.setBoolean(1, true);
        try (var rs = stmt.executeQuery()) {
            final var result = new ArrayList<String>();
            while (rs.next()) {
                result.add(rs.getString("username"));
            }
            return result;
        }
    }
});

System.out.println("Active users: " + users);
```

### Spring Boot Integration

```java

@Configuration
public class DataSourceConfig {

    @Bean
    public RotatingDataSource rotatingDataSource() {
        final var factory = secret -> {
            var config = new HikariConfig();
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
final var rotatingDs = new RotatingDataSource("my-db-secret", factory, 60L);

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
var primaryDs = new RotatingDataSource(
    "primary-db-secret",
    secret -> createHikariDataSource(secret, false),
    60L
);

// Read replica (read-only)
var replicaDs = new RotatingDataSource(
    "replica-db-secret",
    secret -> {
        var config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
        config.setUsername(secret.username());
        config.setPassword(secret.password());
        config.setReadOnly(true);
        config.setMaximumPoolSize(20);
        return new HikariDataSource(config);
    },
    60L
);
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
var customDetector = Retry.AuthErrorDetector.custom(e ->
    e.getErrorCode() == 12345 ||
    e.getMessage().contains("authentication failed")
);

// Combine with default detector
var combined = Retry.AuthErrorDetector.defaultDetector().or(customDetector);
```

## Testing

The library includes comprehensive tests using Testcontainers for PostgreSQL and MySQL:

```bash
# Run all tests
mvn -pl sm-core test

# Run specific test
mvn -pl sm-core test -Dtest=RotatingDataSourceTest

# Run integration tests only
mvn -pl sm-core test -Dtest=*IntegrationTest
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

## Related

- See [connection-pool-examples/README.md](../rotating-data-source-connection-pool-examples/README.md) for complete runnable examples with HikariCP
- See [README.md](../README.md) for overall project documentation
