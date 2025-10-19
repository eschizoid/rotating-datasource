package com.example.rotatingdatasource.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

/**
 * R2DBC ConnectionFactory wrapper that automatically survives credential rotations with
 * built-in retry and optional proactive refresh, mirroring RotatingDataSource features.
 *
 * <h2>Basic Usage</h2>
 *
 * <pre>{@code
 * var rotatingCf = RotatingConnectionFactory.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> createR2dbcPool(secret))
 *     .build();
 *
 * return Mono.usingWhen(
 *     Mono.from(rotatingCf.create()),
 *     conn -> Mono.from(conn.createStatement("SELECT 1").execute())
 *                 .flatMap(r -> Mono.from(r.map((row, md) -> row.get(0, Integer.class)))),
 *     conn -> Mono.from(conn.close()));
 * }</pre>
 *
 * <h2>Proactive Secret Refresh</h2>
 *
 * <pre>{@code
 * var rotatingCf = RotatingConnectionFactory.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> createR2dbcPool(secret))
 *     .refreshIntervalSeconds(60L)
 *     .build();
 * }</pre>
 *
 * <h2>Dual-Password Overlap for RDS</h2>
 *
 * <pre>{@code
 * var rotatingCf = RotatingConnectionFactory.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> createR2dbcPool(secret))
 *     .refreshIntervalSeconds(60L)
 *     .overlapDuration(Duration.ofHours(24))
 *     .build();
 * }</pre>
 *
 * <h2>Full Configuration</h2>
 *
 * <pre>{@code
 * var rotatingCf = RotatingConnectionFactory.builder()
 *     .secretId("my-db-secret")
 *     .factory(secret -> createR2dbcPool(secret))
 *     .refreshIntervalSeconds(60L)
 *     .authErrorDetector(R2dbcAuthErrorDetector.defaultDetector())
 *     .overlapDuration(Duration.ofMinutes(15))
 *     .gracePeriod(Duration.ofSeconds(60))
 *     .build();
 * }</pre>
 */
public final class RotatingConnectionFactory implements ConnectionFactory {

  private static final System.Logger logger =
      System.getLogger(RotatingConnectionFactory.class.getName());

  private final String secretId;
  private final ConnectionFactoryProvider factory;
  private final R2dbcAuthErrorDetector authErrorDetector;
  private final Duration overlapDuration;
  private final Duration gracePeriod;

  private final AtomicReference<ConnectionFactory> primaryFactory = new AtomicReference<>();
  private final AtomicReference<ConnectionFactory> secondaryFactory = new AtomicReference<>();
  private final AtomicReference<Instant> secondaryExpiresAt = new AtomicReference<>();
  private final AtomicReference<String> cachedVersionId = new AtomicReference<>();
  private final AtomicBoolean isRefreshing = new AtomicBoolean(false);

  private reactor.core.Disposable scheduler;

  private RotatingConnectionFactory(Builder builder) {
    this.secretId = builder.secretId;
    this.factory = builder.factory;
    this.authErrorDetector = builder.authErrorDetector;
    this.overlapDuration = builder.overlapDuration;
    this.gracePeriod = builder.gracePeriod;

    primaryFactory.set(createConnectionFactory());

    if (builder.refreshIntervalSeconds > 0) {
      this.scheduler =
          reactor.core.scheduler.Schedulers.boundedElastic()
              .schedulePeriodically(
                  this::checkAndRefresh,
                  Duration.ofSeconds(builder.refreshIntervalSeconds).toMillis(),
                  Duration.ofSeconds(builder.refreshIntervalSeconds).toMillis(),
                  TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Creates a new builder instance.
   *
   * @return new builder for configuring a RotatingConnectionFactory
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for creating RotatingConnectionFactory instances with fluent configuration.
   *
   * <h3>Example: Minimal Configuration</h3>
   *
   * <pre>{@code
   * var rotatingCf = RotatingConnectionFactory.builder()
   *     .secretId("my-db-secret")
   *     .factory(secret -> createR2dbcPool(secret))
   *     .build();
   * }</pre>
   *
   * <h3>Example: Production Configuration</h3>
   *
   * <pre>{@code
   * var rotatingCf = RotatingConnectionFactory.builder()
   *     .secretId("prod-db-secret")
   *     .factory(secret -> createR2dbcPool(secret))
   *     .refreshIntervalSeconds(60L)
   *     .overlapDuration(Duration.ofHours(24))
   *     .gracePeriod(Duration.ofMinutes(2))
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private String secretId;
    private ConnectionFactoryProvider factory;
    private long refreshIntervalSeconds = 0L;
    private R2dbcAuthErrorDetector authErrorDetector = R2dbcAuthErrorDetector.defaultDetector();
    private Duration overlapDuration = Duration.ofMinutes(15);
    private Duration gracePeriod = Duration.ofSeconds(60);

    /**
     * Sets the AWS Secrets Manager secret ID (required).
     *
     * @param secretId the secret identifier
     * @return this builder
     */
    public Builder secretId(String secretId) {
      this.secretId = secretId;
      return this;
    }

    /**
     * Sets the ConnectionFactory factory function (required).
     *
     * @param factory function that creates a ConnectionFactory from a DbSecret
     * @return this builder
     */
    public Builder factory(ConnectionFactoryProvider factory) {
      this.factory = factory;
      return this;
    }

    /**
     * Sets the proactive refresh interval in seconds.
     *
     * <p>When set to a value greater than 0, the factory will periodically check for secret
     * version changes and automatically refresh credentials.
     *
     * <p>Default: 0 (disabled)
     *
     * @param refreshIntervalSeconds interval in seconds (0 to disable)
     * @return this builder
     */
    public Builder refreshIntervalSeconds(long refreshIntervalSeconds) {
      this.refreshIntervalSeconds = refreshIntervalSeconds;
      return this;
    }

    /**
     * Sets the authentication error detector.
     *
     * <p>Default: {@link R2dbcAuthErrorDetector#defaultDetector()}
     *
     * @param authErrorDetector custom auth error detection logic
     * @return this builder
     */
    public Builder authErrorDetector(R2dbcAuthErrorDetector authErrorDetector) {
      this.authErrorDetector = authErrorDetector;
      return this;
    }

    /**
     * Sets the dual-password overlap duration.
     *
     * <p>During this period after a refresh, both old and new credentials remain valid. This
     * supports zero-downtime rotation for databases like AWS RDS that support dual passwords.
     *
     * <p>Default: 15 minutes
     *
     * @param overlapDuration how long to keep old credentials valid
     * @return this builder
     */
    public Builder overlapDuration(Duration overlapDuration) {
      this.overlapDuration = overlapDuration;
      return this;
    }

    /**
     * Sets the grace period before disposing the old ConnectionFactory when not using overlap.
     *
     * <p>After a credential refresh (when overlap is zero), the old factory will be kept alive for
     * this duration to allow in-flight operations to complete.
     *
     * <p>Default: 60 seconds
     *
     * @param gracePeriod how long to wait before disposing old factories
     * @return this builder
     */
    public Builder gracePeriod(Duration gracePeriod) {
      this.gracePeriod = gracePeriod;
      return this;
    }

    /**
     * Builds the RotatingConnectionFactory instance.
     *
     * <p>Immediately fetches the secret and creates the initial ConnectionFactory. If the secret
     * doesn't exist or the factory fails, this method throws.
     *
     * @return configured RotatingConnectionFactory
     * @throws IllegalStateException if required fields are not set
     * @throws RuntimeException if initial ConnectionFactory creation fails
     */
    public RotatingConnectionFactory build() {
      if (secretId == null || secretId.isBlank())
        throw new IllegalStateException("secretId is required");
      if (factory == null) throw new IllegalStateException("factory is required");
      return new RotatingConnectionFactory(this);
    }
  }

  /**
   * Creates a new R2DBC Connection.
   *
   * <p>Before acquiring the connection, this method checks for a newer secret version and refreshes
   * credentials if needed. It will retry once on authentication errors by resetting credentials, and
   * will also retry on transient connection errors using an exponential backoff policy.
   *
   * @return a Mono emitting the acquired Connection
   */
  @Override
  public Mono<Connection> create() {
    return checkLatestVersionAndRefreshIfNeeded()
        .then(Mono.defer(this::tryGetConnectionWithFallback))
        .retryWhen(authRetrySpec())
        .retryWhen(transientRetrySpec());
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return primaryFactory.get().getMetadata();
  }

  /**
   * Forces an immediate credential refresh.
   *
   * <p>Creates a new underlying ConnectionFactory using the latest secret version and swaps it in.
   * If overlapDuration is non-zero, the previous factory remains available until the overlap window
   * expires; otherwise it is disposed after the configured gracePeriod.
   *
   * <p>Concurrent calls are coalesced; if a refresh is already in progress, subsequent calls return
   * immediately.
   *
   * @return a Mono that completes when the refresh operation has been scheduled/applied
   */
  public Mono<Void> reset() {
    return Mono.defer(
        () -> {
          if (!isRefreshing.compareAndSet(false, true)) {
            logger.log(System.Logger.Level.DEBUG, "Reset already in progress");
            return Mono.empty();
          }

          return Mono.fromRunnable(
                  () -> {
                    logger.log(
                        System.Logger.Level.INFO,
                        "Refreshing credentials for secret: {0}",
                        secretId);

                    final var oldFactory = primaryFactory.get();
                    final var newFactory = createConnectionFactory();
                    primaryFactory.set(newFactory);

                    if (!overlapDuration.isZero()) {
                      final var previousSecondary = secondaryFactory.getAndSet(oldFactory);
                      if (previousSecondary != null) {
                        disposeFactory(previousSecondary);
                      }

                      secondaryExpiresAt.set(Instant.now().plus(overlapDuration));

                      Mono.delay(overlapDuration, Schedulers.boundedElastic())
                          .doOnNext(
                              __ -> {
                                final var expired = secondaryFactory.getAndSet(null);
                                if (expired != null) {
                                  disposeFactory(expired);
                                  logger.log(
                                      System.Logger.Level.INFO,
                                      "Closed secondary factory after overlap");
                                }
                                secondaryExpiresAt.set(null);
                              })
                          .subscribe();
                    } else {
                      Mono.delay(gracePeriod, Schedulers.boundedElastic())
                          .doOnNext(
                              __ -> {
                                disposeFactory(oldFactory);
                                logger.log(
                                    System.Logger.Level.INFO,
                                    "Closed old factory after grace period");
                              })
                          .subscribe();
                    }

                    logger.log(System.Logger.Level.INFO, "Credentials refreshed successfully");
                  })
              .doFinally(__ -> isRefreshing.set(false))
              .then();
        });
  }

  /**
   * Shuts down resources associated with this rotating factory.
   *
   * <p>Cancels the proactive refresh scheduler if enabled and disposes the current and any
   * secondary ConnectionFactory instances.
   *
   * @return a Mono that completes after resources have been disposed
   */
  public Mono<Void> shutdown() {
    return Mono.fromRunnable(
        () -> {
          if (scheduler != null) {
            scheduler.dispose();
          }

          disposeFactory(primaryFactory.get());
          disposeFactory(secondaryFactory.get());
        });
  }

  private Mono<Connection> tryGetConnectionWithFallback() {
    return Mono.from(primaryFactory.get().create())
        .cast(Connection.class)
        .onErrorResume(
            e -> {
              if (authErrorDetector.isAuthError(e) && isOverlapActive()) {
                logger.log(
                    System.Logger.Level.INFO,
                    "Primary auth failed, trying secondary during overlap");
                return Mono.from(secondaryFactory.get().create()).cast(Connection.class);
              }
              return Mono.error(e);
            });
  }

  private Retry authRetrySpec() {
    return Retry.max(1)
        .filter(authErrorDetector::isAuthError)
        .doBeforeRetry(
            signal -> {
              logger.log(System.Logger.Level.WARNING, "Auth error detected, resetting credentials");
              reset().block();
            });
  }

  private Retry transientRetrySpec() {
    return Retry.backoff(20, Duration.ofSeconds(5))
        .maxBackoff(Duration.ofMinutes(1))
        .jitter(0.25)
        .filter(R2dbcRetryHelper::isTransientConnectionError)
        .doBeforeRetry(
            signal ->
                logger.log(
                    System.Logger.Level.DEBUG,
                    "Transient error on attempt {0}, retrying...",
                    signal.totalRetries() + 1));
  }

  private void checkAndRefresh() {
    try {
      final var latestVersionId = SecretHelper.getSecretVersion(secretId);
      if (!latestVersionId.equals(cachedVersionId.get())) {
        logger.log(System.Logger.Level.INFO, "Secret version changed, refreshing");
        reset().block();
      }
    } catch (Exception e) {
      logger.log(System.Logger.Level.WARNING, "Failed to check secret version", e);
    }
  }

  private Mono<Void> checkLatestVersionAndRefreshIfNeeded() {
    return Mono.fromCallable(() -> SecretHelper.getSecretVersion(secretId))
        .flatMap(
            latest -> {
              if (!latest.equals(cachedVersionId.get())) {
                logger.log(
                    System.Logger.Level.DEBUG,
                    "Detected new version during connection acquisition");
                return reset();
              }
              return Mono.empty();
            })
        .onErrorResume(
            e -> {
              logger.log(System.Logger.Level.WARNING, "Version check failed", e);
              return Mono.empty();
            });
  }

  private ConnectionFactory createConnectionFactory() {
    final var versionId = SecretHelper.getSecretVersion(secretId);
    cachedVersionId.set(versionId);
    final var secret = SecretHelper.getDbSecret(secretId);
    return factory.create(secret);
  }

  private void disposeFactory(ConnectionFactory factory) {
    if (factory instanceof reactor.core.Disposable disposable) {
      disposable.dispose();
    }
  }

  /**
   * Returns whether the dual-password overlap window is currently active.
   *
   * <p>During overlap, authentication failures on the primary will fall back to acquiring
   * connections from the secondary factory created during the last refresh.
   *
   * @return true if overlap is active, false otherwise
   */
  public boolean isOverlapActive() {
    final var secondary = secondaryFactory.get();
    final var expiresAt = secondaryExpiresAt.get();
    return secondary != null && expiresAt != null && Instant.now().isBefore(expiresAt);
  }

  /**
   * Returns the timestamp when the current overlap window will end, if active.
   *
   * @return Optional of Instant with the overlap expiration time, or empty if no overlap is active
   */
  public Optional<Instant> getOverlapExpiresAt() {
    return java.util.Optional.ofNullable(secondaryExpiresAt.get());
  }
}
