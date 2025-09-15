package com.example.smrotator.core;

import static java.lang.System.Logger.Level.*;

import java.lang.System.Logger;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/**
 * DataSource wrapper that rebuilds its underlying {@link DataSource} from a secret on demand.
 *
 * <p>Useful for automatic recovery after credential rotation; callers can invoke {@link #reset()}
 * to refresh the pool with new credentials. Optionally supports proactive refresh when the secret
 * version changes by enabling a periodic check.
 */
public class RotatingDataSource {

  private final Logger logger = System.getLogger(RotatingDataSource.class.getName());

  private final AtomicReference<DataSource> dataSourceRef;
  private final String secretId;
  private final DataSourceFactory factory;
  private final ScheduledExecutorService scheduler; // nullable when proactive refresh disabled
  private volatile String lastSecretVersion;

  /**
   * Creates a rotating data source bound to a specific secret with no proactive refresh.
   *
   * @param secretId the Secrets Manager secret ID or name
   * @param factory function that creates a {@link DataSource} from a {@link DbSecret}
   */
  public RotatingDataSource(final String secretId, final DataSourceFactory factory) {
    this(secretId, factory, 0L);
  }

  /**
   * Creates a rotating data source with optional proactive refresh.
   *
   * @param secretId the Secrets Manager secret ID or name
   * @param factory function that creates a {@link DataSource} from a {@link DbSecret}
   * @param refreshIntervalSeconds if > 0, periodically checks secret version and refreshes
   */
  public RotatingDataSource(
      final String secretId, final DataSourceFactory factory, final long refreshIntervalSeconds) {
    this.secretId = secretId;
    this.factory = factory;
    this.dataSourceRef = new AtomicReference<>(createDataSource());

    // initialize version
    try {
      this.lastSecretVersion = SecretHelper.getSecretVersion(secretId);
    } catch (final Exception exception) {
      // If the version is unavailable, use null; reset() will update it later.
      logger.log(WARNING, "Failed to get initial secret version.", exception);
      this.lastSecretVersion = null;
    }

    if (refreshIntervalSeconds > 0) {
      this.scheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                final Thread t = new Thread(r, "rotating-ds-refresh");
                t.setDaemon(true);
                return t;
              });
      scheduler.scheduleAtFixedRate(
          this::checkAndRefresh, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
    } else {
      this.scheduler = null;
    }
  }

  /**
   * Builds a new {@link DataSource} by loading the current secret and delegating to the factory.
   *
   * @return a new {@link DataSource}
   * @throws RuntimeException if the secret cannot be loaded or the DataSource cannot be created
   */
  private DataSource createDataSource() {
    return Optional.of(secretId).map(SecretHelper::getDbSecret).map(factory::create).orElseThrow();
  }

  /**
   * Returns the current {@link DataSource} instance.
   *
   * @return the current data source
   */
  public DataSource getDataSource() {
    return dataSourceRef.get();
  }

  /**
   * Replaces the current {@link DataSource} with a newly created instance and closes the previous
   * one if it is {@link AutoCloseable} (e.g., a connection pool). Closing happens asynchronously to
   * avoid blocking callers.
   */
  public void reset() {
    try {
      final var newDs = createDataSource();
      final var oldDs = dataSourceRef.getAndSet(newDs);
      try {
        this.lastSecretVersion = SecretHelper.getSecretVersion(secretId);
      } catch (final Exception ignored) {
        this.lastSecretVersion = null;
      }
      Optional.ofNullable(oldDs)
          .filter(AutoCloseable.class::isInstance)
          .map(AutoCloseable.class::cast)
          .ifPresent(
              ac ->
                  CompletableFuture.runAsync(
                      () -> {
                        try {
                          ac.close();
                        } catch (final Exception ignored) {
                        }
                      }));
      logger.log(INFO, "Pool swapped successfully.");
    } catch (final Exception exception) {
      logger.log(ERROR, "Failed to reset pool.", exception);
    }
  }

  /** Checks the secret version and refreshes the pool if changed. */
  private void checkAndRefresh() {
    try {
      final var current = SecretHelper.getSecretVersion(secretId);
      final var previous = this.lastSecretVersion;
      if (current != null && !current.equals(previous)) {
        logger.log(INFO, "Secret version changed, swapping pool...");
        reset();
      }
    } catch (final Exception exception) {
      logger.log(WARNING, "Failed to check secret version.", exception);
    }
  }

  /** Shuts down the scheduler (if any) and closes the current DataSource if it is AutoCloseable. */
  public void shutdown() {
    if (scheduler != null) scheduler.shutdown();
    final var ds = dataSourceRef.get();
    if (ds instanceof AutoCloseable ac) {
      try {
        ac.close();
      } catch (final Exception ignored) {
      }
    }
  }
}
