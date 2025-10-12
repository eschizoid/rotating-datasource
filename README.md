# Rotating Data Source

A JDBC DataSource that automatically recovers from AWS Secrets Manager credential rotations, with built-in retry logic
for authentication failures.

## Features

- **RotatingDataSource**: A `javax.sql.DataSource` wrapper that can refresh its underlying connection pool on demand or
  proactively
- **Built-in Retry Logic**: Automatic retry on authentication failures with configurable policies (fixed delay or
  exponential backoff)
- **Custom Auth Error Detection**: Pluggable authentication error detection for vendor-specific error codes
- **DbClient**: Thin JDBC client with automatic auth-failure retry
- **Minimal Dependencies**: Only AWS SDK, Jackson, and a JDBC driver
- **ORM-Friendly**: Works seamlessly with Hibernate, JPA, JOOQ, and Spring Data

## Quick Start

### 1. Basic Usage

```java
// Create a rotating data source
final var rotatingDs = new RotatingDataSource(
    "my-db-secret",
    secret -> {
        var config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
        config.setUsername(secret.username());
        config.setPassword(secret.password());
        return new HikariDataSource(config);
    }
);

// Use DbClient for JDBC operations with automatic retry
final var client = new DbClient(rotatingDs);
final var result = client.executeWithRetry(conn -> {
    try (var stmt = conn.createStatement()) {
        var rs = stmt.executeQuery("SELECT COUNT(*) FROM users");
        return rs.next() ? rs.getInt(1) : 0;
    }
});
```

### 2. Custom Retry Policy

```java
// Exponential backoff: 5 attempts, starting at 100ms, up to 30s, with jitter
final var policy = Retry.Policy.exponential(5, 100L);

final var rotatingDs = new RotatingDataSource(
    "my-db-secret",
    factory,
    0L,      // no proactive refresh
    policy   // exponential backoff on auth errors
);
```

### 3. Proactive Refresh

```java
// Check secret version every 60 seconds and refresh if changed
final var rotatingDs = new RotatingDataSource(
    "my-db-secret",
    factory,
    60L  // check every 60 seconds
);
```

### 4. ORM Integration

```java
// Configure your ORM to use RotatingDataSource
final var rotatingDs = new RotatingDataSource("my-db-secret", factory);

// Hibernate/JPA - automatic retry on connection acquisition
final var users = entityManager.createQuery("FROM User", User.class).getResultList();

// Persist operations
entityManager.persist(newUser);
entityManager.flush();

// Note: Retry.authRetry() is only needed for rare edge cases where
// credentials expire during long-running transactions
```

### 5. Vendor-Specific Error Detection

```java
// Oracle-specific auth error detection
final var oracleDetector = Retry.AuthErrorDetector.custom(e ->
    e.getErrorCode() == 1017 ||  // ORA-01017: invalid username/password
    e.getErrorCode() == 28000    // ORA-28000: account is locked
);

final var rotatingDs = new RotatingDataSource(
    "oracle-secret",
    factory,
    0L,
    Retry.RetryPolicy.fixed(2, 50L),
    oracleDetector
);
```

### 6. Retry Observability

```java
// Built-in logging listener
final var listener = Retry.RetryListener.logging();

final var result = Retry.onException(
    () -> executeQuery(),
    SQLException::isTransient,
    () -> reconnect(),
    3, 100L,
    listener
);

// Custom metrics listener
final var metricsListener = (attempt, exception) -> {
    metrics.increment("db.retries",
        "attempt", String.valueOf(attempt),
        "error_type", exception.getClass().getSimpleName()
    );
};
```

## Architecture

### Core Components

- **DbSecret**: Immutable record representing RDS secret JSON structure
- **SecretsManagerProvider**: Lazily configured AWS Secrets Manager client with optional caching
- **SecretHelper**: Fetches and deserializes secrets into `DbSecret`
- **DataSourceFactory**: Functional interface for building `DataSource` from `DbSecret`
- **RotatingDataSource**: `DataSource` wrapper with automatic retry and credential rotation
- **Retry**: Functional retry utilities with configurable policies and observability
- **DbClient**: Thin JDBC client executing operations with auth-failure retry

### Retry Capabilities

The `Retry` class provides three levels of functionality:

1. **Basic Retry**: Fixed delay with custom predicate
   ```java
   Retry.onException(
       () -> executeQuery(),
       e -> e.getSQLState().startsWith("40"),
       () -> reconnect(),
       3, 100L
   );
   ```

2. **Policy-Based Retry**: Fixed delay or exponential backoff
   ```java
   final var policy = Retry.RetryPolicy.exponential(5, 100L);
   Retry.onException(
       () -> executeQuery(),
       SQLException::isTransient,
       () -> {},
       policy
   );
   ```

### Authentication Error Detection

The library detects auth errors using:

- **SQLState codes**: `28000` (invalid authorization), `28P01` (PostgreSQL invalid password)
- **Message keywords**: "access denied", "authentication failed", "invalid password", etc.

You can customize detection for specific vendors:

```java
// MySQL example
final var mysqlDetector = Retry.AuthErrorDetector.custom(e ->
    e.getErrorCode() == 1045 ||  // Access denied
    e.getErrorCode() == 1044     // Access denied for database
);

// Combine with default heuristics
final var combined = Retry.AuthErrorDetector.defaultDetector().or(mysqlDetector);
```

## Configuration

### AWS Credentials

Configure via system properties or environment variables:

- `aws.region` / `AWS_REGION` (default: us-east-1)
- `aws.sm.endpoint` / `AWS_SM_ENDPOINT` (for Localstack)
- `aws.accessKeyId` / `AWS_ACCESS_KEY_ID`
- `aws.secretAccessKey` / `AWS_SECRET_ACCESS_KEY`

### Secret Caching

Enable optional caching to reduce AWS API calls:

- `aws.sm.cache.ttl.millis` / `AWS_SM_CACHE_TTL_MILLIS` (default: 0 = disabled)

Example: `aws.sm.cache.ttl.millis=300000` (5 minutes)

## Testing

The project includes comprehensive tests using:

- **Localstack**: Simulates AWS Secrets Manager
- **Testcontainers**: PostgreSQL database
- **JUnit 5**: Test framework

Run tests:

```bash
mvn clean test
```

## Spring Boot Integration

```java

@Configuration
public class DataSourceConfig {

    @Bean
    public RotatingDataSource rotatingDataSource() {
        return new RotatingDataSource(
            "my-db-secret",
            secret -> {
                var config = new HikariConfig();
                config.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
                config.setUsername(secret.username());
                config.setPassword(secret.password());
                config.setMaximumPoolSize(10);
                return new HikariDataSource(config);
            },
            60L,  // proactive refresh every 60 seconds
            Retry.Policy.exponential(5, 200L)
        );
    }

    @Bean
    public DataSource dataSource(RotatingDataSource rotatingDataSource) {
        return rotatingDataSource;
    }

    @Bean
    public DbClient dbClient(RotatingDataSource rotatingDataSource) {
        return new DbClient(rotatingDataSource);
    }
}
```

## Hibernate/JPA Configuration

```java
// Configure Hibernate to use RotatingDataSource
final var sessionFactory = new Configuration()
    .setProperty("hibernate.connection.provider_class", 
        "org.hibernate.connection.DatasourceConnectionProvider")
    .setProperty("hibernate.connection.datasource", rotatingDataSource)
    // other Hibernate settings
    .buildSessionFactory();

// Connection acquisition automatically retries on auth failures
try (var session = sessionFactory.openSession()) {
    // No explicit retry needed - handled by RotatingDataSource
    var users = session.createQuery("FROM User", User.class).list();
}
```

## FAQ

**Q: When does credential rotation happen?**  
A: Rotation happens in AWS Secrets Manager (via Lambda). This library detects auth failures and refreshes the DataSource
with new credentials.

**Q: Does RotatingDataSource automatically retry?**  
A: Yes! `getConnection()` automatically retries on authentication failures using the configured retry policy (default: 2
attempts with 50ms delay).

**Q: Does RotatingDataSource rotate proactively?**  
A: It can. By default, it refreshes on demand when you call `reset()` (triggered automatically on auth failures). Enable
proactive refresh by passing a positive `refreshIntervalSeconds` to check the secret version periodically.

**Q: Is there connection leakage during reset?**  
A: No. `RotatingDataSource` closes the previous `DataSource` if it implements `AutoCloseable` (HikariDataSource does).
Existing connections remain valid until closed by your code or the pool.

**Q: Can I customize the retry policy?**  
A: Yes! Use `Policy.fixed()` or `Policy.exponential()` factory methods, or create a custom policy with full control over 
attempts, delays, backoff multiplier, and jitter.

**Q: Does this require a specific connection pool?**  
A: No. Any `DataSource` can be created by your `DataSourceFactory`. HikariCP is used in examples and tests, but any
pool (DBCP2, C3P0, etc.) works.

**Q: Can I use DbClient with ORMs?**  
A: Yes, but configure your ORM to use `RotatingDataSource` as its `DataSource` so credential rotation is handled
transparently. Use `DbClient` for ad-hoc JDBC operations alongside the ORM. For ORM operations that may fail with auth
errors, use `Retry.authRetry()`.

**Q: How do I add metrics for retry attempts?**

A: Use `RetryListener` to hook into retry events:

```java
final var listener = (attempt, exception) -> {
    metrics.increment("db.retries",
        "attempt", String.valueOf(attempt),
        "error_type", exception.getClass().getSimpleName()
    );
};

Retry.onException(supplier, shouldRetry, beforeRetry, 3, 100L, listener);
```

**Q: What if my database vendor uses custom error codes?**  
A: Implement a custom `AuthErrorDetector`:

```java
final var customDetector = Retry.AuthErrorDetector.custom(e ->
    e.getErrorCode() == 1234  // your vendor's auth error code
);

final var rotatingDs = new RotatingDataSource(secretId, factory, 0L, policy, customDetector);
```

**Q: Does this work with read replicas?**  
A: Yes. Create separate `RotatingDataSource` instances for primary and replica secrets, each with its own factory.
