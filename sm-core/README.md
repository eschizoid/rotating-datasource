# sm-core

Core library that implements a rotating JDBC DataSource powered by AWS Secrets Manager. This module contains the
reusable building blocks; sample runnable apps live under connection-pool-examples.

## What’s inside

- `DbSecret` — immutable record mapping AWS RDS-like secret JSON (username, password, engine, host, port, dbname)
- `SecretsManagerProvider` — builds/configures the AWS Secrets Manager client
- `SecretHelper` — loads and parses a secret into `DbSecret`
- `DataSourceFactory` — functional interface to build a `javax.sql.DataSource` from a `DbSecret`
- `RotatingDataSource` — wraps a `DataSource` and can `reset()` itself when credentials change
- `Retry` — minimal helper used by `DbClient`
- `DbClient` — executes JDBC operations and retries once on auth failures (triggers rotation)

## Requirements

- Java 17+
- Maven 3.9+

## Build this module

From the repository root:

- mvn -q -pl sm-core -am -DskipTests clean package — build only sm-core
- Artifact: sm-core/target/sm-core-1.0.0-SNAPSHOT.jar

## Using the core in your code

Example with HikariCP:

```java

import com.example.smrotator.core.DataSourceFactory;
import com.example.smrotator.core.DbClient;
import com.example.smrotator.core.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

final DataSourceFactory factory = secret -> {
    final var url = "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname());
    final var cfg = new HikariConfig();
    cfg.setJdbcUrl(url);
    cfg.setUsername(secret.username());
    cfg.setPassword(secret.password());
    cfg.setMaximumPoolSize(10);
    return new HikariDataSource(cfg);
};

final var rotating = new RotatingDataSource("your/secret/id", factory);
final var client = new DbClient(rotating);

final var result = client.executeWithRetry(conn -> {
    try (var st = conn.createStatement(); var rs = st.executeQuery("SELECT 1")) {
        rs.next();
        return rs.getString(1);
    }
});
System.out.println(result);
```

## Configuration

`SecretsManagerProvider` reads configuration from system properties or environment variables:

- `aws.region` or `AWS_REGION` — region (default: us-east-1)
- `aws.sm.endpoint` or `AWS_SM_ENDPOINT` — endpoint override (e.g., Localstack)
- `aws.accessKeyId` or `AWS_ACCESS_KEY_ID` — explicit credentials (optional)
- `aws.secretAccessKey` or `AWS_SECRET_ACCESS_KEY` — explicit credentials (optional)
- `aws.sm.cache.ttl.millis` or `AWS_SM_CACHE_TTL_MILLIS` — optional in-memory cache TTL for getSecret/getSecretVersion. Default 0 (disabled). Set to a small value (e.g., 500–2000) to reduce SDK calls under high load.

If accessKey/secretKey are not provided, the default AWS provider chain is used.

Notes on caching:
- The cache is per-process, in-memory, and not size-bounded. It stores the full secret string and version id per secretId for the configured TTL.
- Caching getSecretVersion delays detection of rotations until the TTL expires. Keep TTL low if you depend on rapid version checks (e.g., when using proactive refresh).
- You can clear the cache programmatically via `SecretsManagerProvider.resetCache()` or rebuild the client (which also clears the cache) via `resetClient()`.

## How rotation works

1) `DbClient` runs an operation using a connection from `RotatingDataSource`.
2) If it sees an auth failure (`SQLState` 28000 or typical messages), it calls `RotatingDataSource.reset()`.
3) `SecretHelper` fetches the latest secret, `DataSourceFactory` builds a new `DataSource`, the previous pool is closed
   when applicable.
4) The operation is retried once against the new `DataSource`.

Optional proactive refresh: use `RotatingDataSource(secretId, factory, refreshIntervalSeconds)` with a positive interval
to check the secret version periodically and refresh when it changes.

## Retry: onException vs authRetry

- `Retry.onException`: Generic, predicate-based helper for operations that throw checked SQLExceptions. Used by
  DbClient. Prefer this in low-level JDBC code when you control the exception type and want to customize attempts/delay
  and the retry predicate.
- `Retry.authRetry`: Convenience for higher layers (ORMs/service methods) where exceptions are often wrapped in
  `RuntimeException`. It looks for an authentication-related `SQLException` in the cause chain, calls
  `RotatingDataSource.reset()`, and retries once. Use this in the ORM examples or similar service-layer code.

If you only use `DbClient`, you may never call `authRetry` directly. If you only wrap ORM calls, you may never need
`onException`. They are small and independent by design.

## Testing

This module doesn’t require Docker to compile, but it includes optional integration tests that use Testcontainers and therefore require Docker when enabled.

- Integration tests in this module: `SecretsManagerProviderTest` (uses Localstack) and `DataSourceFactoryTest` (uses PostgreSQL/MySQL containers via HikariCP).
- They will auto-skip when Docker is not available. You can also disable them explicitly with `-Dtests.integration.disable=true`.

Commands (from repository root):

- mvn -q clean test — runs all tests; integration tests run only if Docker is available
- mvn -q -Dtests.integration.disable=true clean test — force-skip integration tests

## See also

- Project overview and usage: [README.md](../README.md)
- Example apps with ready-to-run pools: [connection-pool-examples/README.md](../connection-pool-examples/README.md)
