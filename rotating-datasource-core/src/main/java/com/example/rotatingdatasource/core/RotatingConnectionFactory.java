package com.example.rotatingdatasource.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

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
                  TimeUnit.SECONDS);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String secretId;
    private ConnectionFactoryProvider factory;
    private long refreshIntervalSeconds = 0L;
    private R2dbcAuthErrorDetector authErrorDetector = R2dbcAuthErrorDetector.defaultDetector();
    private Duration overlapDuration = Duration.ZERO;
    private Duration gracePeriod = Duration.ofSeconds(60);

    public Builder secretId(String secretId) {
      this.secretId = secretId;
      return this;
    }

    public Builder factory(ConnectionFactoryProvider factory) {
      this.factory = factory;
      return this;
    }

    public Builder refreshIntervalSeconds(long refreshIntervalSeconds) {
      this.refreshIntervalSeconds = refreshIntervalSeconds;
      return this;
    }

    public Builder authErrorDetector(R2dbcAuthErrorDetector authErrorDetector) {
      this.authErrorDetector = authErrorDetector;
      return this;
    }

    public Builder overlapDuration(Duration overlapDuration) {
      this.overlapDuration = overlapDuration;
      return this;
    }

    public Builder gracePeriod(Duration gracePeriod) {
      this.gracePeriod = gracePeriod;
      return this;
    }

    public RotatingConnectionFactory build() {
      if (secretId == null || secretId.isBlank())
        throw new IllegalStateException("secretId is required");
      if (factory == null) throw new IllegalStateException("factory is required");
      return new RotatingConnectionFactory(this);
    }
  }

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

  private boolean isOverlapActive() {
    final var secondary = secondaryFactory.get();
    final var expiresAt = secondaryExpiresAt.get();
    return secondary != null && expiresAt != null && Instant.now().isBefore(expiresAt);
  }
}
