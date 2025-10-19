# Rotating Data Source

A JDBC DataSource that automatically recovers from AWS Secrets Manager credential rotations. It swaps the underlying
connection pool when secrets change and transparently retries connection acquisition on authentication/transient errors.

## Features

- AWS RDS ready: works with Secrets Manager rotation for PostgreSQL and MySQL
- RotatingDataSource: a javax.sql.DataSource that refreshes its pool reactively (on auth errors) and/or proactively
- Built-in retry: automatic retry on authentication/transient connection failures with configurable policies
- Pluggable auth error detection for vendor-specific codes
- Minimal dependencies: AWS SDK, Jackson, your JDBC driver / connection pool
- ORM-friendly: drop-in DataSource for Hibernate, JPA, Spring Data, jOOQ (no special wrappers needed)

## Quick Start

### 1) Create with the builder (HikariCP example)

```java
import com.example.rotatingdatasource.core.RotatingDataSource;
import com.example.rotatingdatasource.core.DataSourceFactoryProvider;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

DataSourceFactory factory = secret -> {
  var cfg = new HikariConfig();
  cfg.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
  cfg.setUsername(secret.username());
  cfg.setPassword(secret.password());
  cfg.setMaximumPoolSize(10);
  return new HikariDataSource(cfg);
};

var ds = RotatingDataSource.builder()
    .secretId("my-db-secret")
    .factory(factory)
    .refreshIntervalSeconds(60)  // optional proactive check
    .build();
```

Use ds like any DataSource. Connection acquisition automatically retries on auth and transient errors.

### 2) Customize retry policy

```java
import com.example.rotatingdatasource.core.Retry;

var ds = RotatingDataSource.builder()
    .secretId("my-db-secret")
    .factory(factory)
    .retryPolicy(Retry.Policy.exponential(7, 200L))  // attempts, initial delay (ms)
    .build();
```

### 3) ORM wiring

- Inject RotatingDataSource as the ORM’s DataSource.
- ORMs inherit the same connection-acquisition retry. You typically do not need to call Retry.authRetry().

```java
var ds = RotatingDataSource.builder().secretId("my-db-secret").factory(factory).build();
var emf = Persistence.createEntityManagerFactory("app", Map.of("jakarta.persistence.nonJtaDataSource", ds));
```

## RDS + Secrets Manager Integration

Rotation is performed by AWS Secrets Manager (often via a Lambda). This library:
- Checks the latest secret version and swaps to a new pool when it changes.
- Retries connection acquisition during/after the swap.
- Supports two cutover modes:
  - overlapDuration (default 15 minutes): keep the old pool as a temporary secondary and fall back to it on auth failures during the overlap window. Use for engines/procedures with dual-valid passwords (e.g., some MySQL/Aurora flows).
  - gracePeriod (default 60 seconds): when overlapDuration is zero, keep the old pool open long enough for in-flight work to finish. Recommended for PostgreSQL where the old password becomes invalid immediately.

PostgreSQL example (no dual password):

```java
var ds = RotatingDataSource.builder()
    .secretId("rds/postgres/app")
    .factory(factory)
    .overlapDuration(Duration.ZERO)
    .gracePeriod(Duration.ofSeconds(60))
    .refreshIntervalSeconds(60)
    .build();
```

MySQL/Aurora example (dual-password overlap in your rotation procedure):

```java
var ds = RotatingDataSource.builder()
    .secretId("rds/mysql/app")
    .factory(factory)
    .overlapDuration(Duration.ofHours(24))  // fallback to old pool if new creds fail during overlap
    .gracePeriod(Duration.ofSeconds(60))    // used only when overlap is zero
    .refreshIntervalSeconds(60)
    .build();
```

## Configuration

- aws.region / AWS_REGION (default: us-east-1)
- aws.sm.endpoint / AWS_SM_ENDPOINT (Localstack)
- aws.accessKeyId / AWS_ACCESS_KEY_ID
- aws.secretAccessKey / AWS_SECRET_ACCESS_KEY
- aws.sm.cache.ttl.millis / AWS_SM_CACHE_TTL_MILLIS (default 0 = disabled; caches SecretManager calls)

## FAQ

- Does it automatically retry? Yes. getConnection() is wrapped with transient/auth retry using the configured policy
  (default builder policy: Retry.Policy.exponential(10, 1000L)).
- Proactive refresh? Optional. Set refreshIntervalSeconds > 0 to poll the secret version and refresh in the background.
- Any pool requirement? No. Your DataSourceFactory can create HikariCP, Tomcat JDBC, DBCP2, etc.
- ORMs? Just point them at the RotatingDataSource. Retry.authRetry is only for rare long-running units of work that
  straddle a rotation and surface an auth-related SQLException.

## Spring Boot example

```java
@Bean
DataSourceFactory dataSourceFactory() { /* build HikariDataSource from secret */ }

@Bean
DataSource dataSource(DataSourceFactory factory) {
  return RotatingDataSource.builder()
      .secretId(System.getProperty("db.secretId"))
      .factory(factory)
      .refreshIntervalSeconds(60)
      .build();
}
```

## See also

- Core module docs: [README.md](rotating-datasource-core/README.md)
- ORM examples: [README.md](rotating-datasource-core/README.md)
- Connection pool examples: [README.md](rotating-datasource-core/README.md)
