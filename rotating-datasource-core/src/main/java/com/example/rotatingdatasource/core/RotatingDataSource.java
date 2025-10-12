package com.example.rotatingdatasource.core;

import static com.example.rotatingdatasource.core.Retry.*;
import static java.lang.System.Logger.Level.*;

import com.example.rotatingdatasource.core.Retry.Policy;
import java.io.PrintWriter;
import java.lang.System.Logger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/**
 * DataSource wrapper that automatically recovers from credential rotations with built-in retry
 * logic for authentication failures.
 *
 * <h2>Basic Usage</h2>
 *
 * <pre>{@code
 * var rotatingDs = RotatingDataSource.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> {
 *         var config = new HikariConfig();
 *         config.setJdbcUrl("jdbc:postgresql://" + secret.host() + ":" + secret.port() + "/" + secret.dbname());
 *         config.setUsername(secret.username());
 *         config.setPassword(secret.password());
 *         return new HikariDataSource(config);
 *     })
 *     .build();
 *
 * try (var conn = rotatingDs.getConnection()) {
 *     // automatic retry on auth failures
 * }
 * }</pre>
 *
 * <h2>Custom Retry Policy</h2>
 *
 * <pre>{@code
 * var rotatingDs = RotatingDataSource.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> createHikariDataSource(secret))
 *     .retryPolicy(Retry.Policy.exponential(5, 100L))
 *     .build();
 * }</pre>
 *
 * <h2>Proactive Secret Refresh</h2>
 *
 * <pre>{@code
 * var rotatingDs = RotatingDataSource.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> createHikariDataSource(secret))
 *     .refreshIntervalSeconds(60L)
 *     .build();
 * }</pre>
 *
 * <h2>Dual-Password Overlap for RDS</h2>
 *
 * <pre>{@code
 * var rotatingDs = RotatingDataSource.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> createHikariDataSource(secret))
 *     .refreshIntervalSeconds(60L)
 *     .overlapDuration(Duration.ofHours(24))
 *     .build();
 * }</pre>
 *
 * <h2>Full Configuration</h2>
 *
 * <pre>{@code
 * var rotatingDs = RotatingDataSource.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> createHikariDataSource(secret))
 *     .refreshIntervalSeconds(60L)
 *     .retryPolicy(Retry.Policy.exponential(5, 200L))
 *     .authErrorDetector(Retry.AuthErrorDetector.custom(e -> e.getErrorCode() == 1017))
 *     .overlapDuration(Duration.ofHours(24))
 *     .gracePeriod(Duration.ofMinutes(2))
 *     .build();
 * }</pre>
 */
public final class RotatingDataSource implements DataSource {

  private static final Logger logger = System.getLogger(RotatingDataSource.class.getName());

  private final String secretId;
  private final DataSourceFactory factory;
  private final AuthErrorDetector authErrorDetector;
  private final Duration overlapDuration;
  private final Duration gracePeriod;

  private final AtomicReference<DataSource> primaryDataSource = new AtomicReference<>();
  private final AtomicReference<DataSource> secondaryDataSource = new AtomicReference<>();
  private final AtomicReference<Instant> secondaryExpiresAt = new AtomicReference<>();
  private final AtomicReference<String> cachedVersionId = new AtomicReference<>();
  private final AtomicBoolean isRefreshing = new AtomicBoolean(false);
  private final AtomicReference<CompletableFuture<Void>> pendingSecondaryCleanup =
      new AtomicReference<>();
  private final AtomicReference<CompletableFuture<Void>> pendingGracePeriodCleanup =
      new AtomicReference<>();

  private ScheduledExecutorService scheduler;

  private RotatingDataSource(final Builder builder) {
    this.secretId = builder.secretId;
    this.factory = builder.factory;
    long refreshIntervalSeconds = builder.refreshIntervalSeconds;
    this.authErrorDetector = builder.authErrorDetector;
    this.overlapDuration = builder.overlapDuration;
    this.gracePeriod = builder.gracePeriod;

    primaryDataSource.set(createDataSource());

    if (refreshIntervalSeconds > 0) {
      scheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                var t = new Thread(r, "RotatingDataSource-" + secretId);
                t.setDaemon(true);
                return t;
              });
      scheduler.scheduleAtFixedRate(
          this::checkAndRefresh, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
    }
  }

  /**
   * Creates a new builder instance.
   *
   * @return new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for creating {@link RotatingDataSource} instances with fluent configuration.
   *
   * <h3>Example: Minimal Configuration</h3>
   *
   * <pre>{@code
   * var rotatingDs = RotatingDataSource.builder()
   *     .secretId("my-db-secret")
   *     .factory(secret -> createHikariDataSource(secret))
   *     .build();
   * }</pre>
   *
   * <h3>Example: Production Configuration</h3>
   *
   * <pre>{@code
   * var rotatingDs = RotatingDataSource.builder()
   *     .secretId("prod-db-secret")
   *     .factory(secret -> createHikariDataSource(secret))
   *     .refreshIntervalSeconds(60L)
   *     .retryPolicy(Retry.Policy.exponential(7, 250L))
   *     .overlapDuration(Duration.ofHours(24))
   *     .gracePeriod(Duration.ofMinutes(2))
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private String secretId;
    private DataSourceFactory factory;
    private long refreshIntervalSeconds = 0L;
    private Policy retryPolicy = Policy.exponential(20, 15_000L);
    private AuthErrorDetector authErrorDetector = AuthErrorDetector.defaultDetector();
    private Duration overlapDuration = Duration.ZERO;
    private Duration gracePeriod = Duration.ofSeconds(60);

    private Builder() {}

    /**
     * Sets the AWS Secrets Manager secret ID (required).
     *
     * @param secretId the secret identifier
     * @return this builder
     */
    public Builder secretId(final String secretId) {
      this.secretId = secretId;
      return this;
    }

    /**
     * Sets the DataSource factory function (required).
     *
     * @param factory function that creates DataSource from secret
     * @return this builder
     */
    public Builder factory(final DataSourceFactory factory) {
      this.factory = factory;
      return this;
    }

    /**
     * Sets the proactive refresh interval in seconds.
     *
     * <p>When set to a value greater than 0, the DataSource will periodically check for secret
     * version changes and automatically refresh credentials.
     *
     * <p>Default: 0 (disabled)
     *
     * @param refreshIntervalSeconds interval in seconds (0 to disable)
     * @return this builder
     */
    public Builder refreshIntervalSeconds(final long refreshIntervalSeconds) {
      this.refreshIntervalSeconds = refreshIntervalSeconds;
      return this;
    }

    /**
     * Sets the retry policy for authentication failures.
     *
     * <p>Default: exponential backoff (20 attempts, starting at 15 seconds)
     *
     * @param retryPolicy the retry policy
     * @return this builder
     */
    public Builder retryPolicy(final Policy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    /**
     * Sets the authentication error detector.
     *
     * <p>Default: {@link AuthErrorDetector#defaultDetector()}
     *
     * @param authErrorDetector custom auth error detection logic
     * @return this builder
     */
    public Builder authErrorDetector(final AuthErrorDetector authErrorDetector) {
      this.authErrorDetector = authErrorDetector;
      return this;
    }

    /**
     * Sets the dual-password overlap duration.
     *
     * <p>During this period after a refresh, both old and new credentials remain valid. This
     * supports zero-downtime rotation for databases like AWS RDS that support dual passwords.
     *
     * <p>Default: ZERO (disabled)
     *
     * @param overlapDuration how long to keep old credentials valid
     * @return this builder
     */
    public Builder overlapDuration(final Duration overlapDuration) {
      this.overlapDuration = overlapDuration;
      return this;
    }

    /**
     * Sets the grace period before closing old connection pools.
     *
     * <p>After a credential refresh (when not using dual-password mode), the old DataSource will be
     * kept alive for this duration to allow in-flight operations to complete.
     *
     * <p>Default: 60 seconds
     *
     * @param gracePeriod how long to wait before closing old pools
     * @return this builder
     */
    public Builder gracePeriod(final Duration gracePeriod) {
      this.gracePeriod = gracePeriod;
      return this;
    }

    /**
     * Builds the RotatingDataSource instance.
     *
     * @return configured RotatingDataSource
     * @throws IllegalStateException if required fields are not set
     */
    public RotatingDataSource build() {
      if (secretId == null || secretId.isBlank())
        throw new IllegalStateException("secretId is required");
      if (factory == null) throw new IllegalStateException("factory is required");
      if (refreshIntervalSeconds < 0)
        throw new IllegalArgumentException("refreshIntervalSeconds must be >= 0");
      if (retryPolicy == null) throw new IllegalStateException("retryPolicy cannot be null");
      if (authErrorDetector == null)
        throw new IllegalStateException("authErrorDetector cannot be null");
      if (overlapDuration == null || overlapDuration.isNegative())
        throw new IllegalArgumentException("overlapDuration must be non-negative");
      if (gracePeriod == null || gracePeriod.isNegative())
        throw new IllegalArgumentException("gracePeriod must be non-negative");
      return new RotatingDataSource(this);
    }
  }

  @Override
  public Connection getConnection() {
    checkLatestVersionAndRefreshIfNeeded();

    return transientRetry(
        () -> authRetry(this::tryGetConnectionWithFallback, this::reset, authErrorDetector),
        Policy.exponential(20, 5_000L));
  }

  @Override
  public Connection getConnection(final String username, final String password) {
    throw new UnsupportedOperationException(
        "Credentials are managed at the pool level via DataSourceFactory");
  }

  public void reset() {
    if (!isRefreshing.compareAndSet(false, true)) {
      logger.log(DEBUG, "Reset already in progress, skipping");
      return;
    }

    try {
      logger.log(INFO, "Refreshing credentials for secret: {0}", secretId);

      final var oldDs = primaryDataSource.get();
      final var newDs = createDataSource();
      primaryDataSource.set(newDs);

      final var pendingGrace = pendingGracePeriodCleanup.getAndSet(null);
      if (pendingGrace != null && !pendingGrace.isDone()) {
        pendingGrace.cancel(true);
        logger.log(DEBUG, "Cancelled pending grace period cleanup from previous rotation");
      }

      if (!overlapDuration.isZero()) {
        final var previousSecondary = secondaryDataSource.getAndSet(oldDs);
        if (previousSecondary != null) {
          closeDataSource(previousSecondary);
        }

        final var expiresAt = Instant.now().plus(overlapDuration);
        secondaryExpiresAt.set(expiresAt);

        final var cleanup =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    Thread.sleep(overlapDuration.toMillis());
                    final var expired = secondaryDataSource.getAndSet(null);
                    if (expired != null) {
                      closeDataSource(expired);
                      logger.log(INFO, "Closed secondary DataSource after overlap period");
                    }
                    secondaryExpiresAt.set(null);
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                  }
                });
        pendingSecondaryCleanup.set(cleanup);
      } else {
        final var cleanup =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    Thread.sleep(gracePeriod.toMillis());
                    closeDataSource(oldDs);
                    logger.log(INFO, "Closed old DataSource after grace period");
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                  }
                });
        pendingGracePeriodCleanup.set(cleanup);
      }

      logger.log(INFO, "Credentials refreshed successfully");
    } catch (final Exception e) {
      logger.log(ERROR, "Failed to refresh credentials", e);
      throw new RuntimeException(e.getMessage(), e);
    } finally {
      isRefreshing.set(false);
    }
  }

  public void shutdown() {
    if (scheduler != null) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) scheduler.shutdownNow();
      } catch (final InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    final var pendingSecondary = pendingSecondaryCleanup.getAndSet(null);
    if (pendingSecondary != null && !pendingSecondary.isDone()) pendingSecondary.cancel(true);

    final var pendingGrace = pendingGracePeriodCleanup.getAndSet(null);
    if (pendingGrace != null && !pendingGrace.isDone()) pendingGrace.cancel(true);

    closeDataSource(primaryDataSource.get());
    closeDataSource(secondaryDataSource.get());
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return primaryDataSource.get().getLogWriter();
  }

  @Override
  public void setLogWriter(final PrintWriter out) throws SQLException {
    primaryDataSource.get().setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(final int seconds) throws SQLException {
    primaryDataSource.get().setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return primaryDataSource.get().getLoginTimeout();
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return primaryDataSource.get().getParentLogger();
  }

  @Override
  public <T> T unwrap(final Class<T> clazz) throws SQLException {
    if (clazz.isInstance(this)) return clazz.cast(this);
    return primaryDataSource.get().unwrap(clazz);
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return iface.isInstance(this) || primaryDataSource.get().isWrapperFor(iface);
  }

  public boolean isOverlapActive() {
    final var secondary = secondaryDataSource.get();
    final var expiresAt = secondaryExpiresAt.get();
    return secondary != null && expiresAt != null && Instant.now().isBefore(expiresAt);
  }

  public Optional<Instant> getOverlapExpiresAt() {
    return Optional.ofNullable(secondaryExpiresAt.get());
  }

  private Connection tryGetConnectionWithFallback() {
    try {
      return primaryDataSource.get().getConnection();
    } catch (final SQLException e) {
      if (authErrorDetector.isAuthError(e) && isOverlapActive()) {
        logger.log(INFO, "Primary auth failed, trying secondary during overlap");
        try {
          return secondaryDataSource.get().getConnection();
        } catch (final SQLException secondaryException) {
          throw new RuntimeException(secondaryException);
        }
      }
      throw new RuntimeException(e);
    }
  }

  private void checkAndRefresh() {
    try {
      final var latestVersionId = SecretHelper.getSecretVersion(secretId);
      if (!latestVersionId.equals(cachedVersionId.get())) {
        logger.log(INFO, "Secret version changed, refreshing DataSource");
        reset();
      }
    } catch (final Exception e) {
      logger.log(WARNING, "Failed to check secret version", e);
    }
  }

  private void checkLatestVersionAndRefreshIfNeeded() {
    try {
      final var latest = SecretHelper.getSecretVersion(secretId);
      final var current = cachedVersionId.get();
      if (!latest.equals(current)) {
        logger.log(DEBUG, "Detected new secret version during connection acquisition");
        reset();
      }
    } catch (final Exception e) {
      logger.log(WARNING, "Version check failed during connection acquisition", e);
    }
  }

  private DataSource createDataSource() {
    final var versionId = SecretHelper.getSecretVersion(secretId);
    cachedVersionId.set(versionId);
    return Optional.of(secretId).map(SecretHelper::getDbSecret).map(factory::create).orElseThrow();
  }

  private void closeDataSource(final DataSource ds) {
    if (ds instanceof AutoCloseable ac) {
      try {
        ac.close();
      } catch (final Exception e) {
        logger.log(WARNING, "Failed to close DataSource", e);
      }
    }
  }
}
