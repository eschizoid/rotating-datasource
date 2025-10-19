package com.example;

import static java.lang.System.Logger.Level.DEBUG;

import com.example.rotatingdatasource.core.DataSourceFactoryProvider;
import com.example.rotatingdatasource.core.DbClient;
import com.example.rotatingdatasource.core.Retry;
import com.example.rotatingdatasource.core.RotatingDataSource;
import java.sql.SQLException;

/** Demo application showing how to query a database while surviving secret rotation. */
public class App {
  private final DbClient client;

  /**
   * Constructs the application with a single RotatingDataSource instance that will be reused for
   * all later calls to {@link #getString()}.
   *
   * @param secretId the secret identifier to load from AWS Secrets Manager
   * @param factory a factory to create DataSources from secrets
   */
  public App(final String secretId, final DataSourceFactoryProvider factory) {
    this(secretId, factory, 0L);
  }

  /**
   * Constructs the application using a RotatingDataSource that can optionally perform proactive
   * refresh checks at a fixed interval.
   *
   * @param secretId the secret identifier to load from AWS Secrets Manager
   * @param factory a factory to create DataSources from secrets
   * @param refreshIntervalSeconds if > 0, enables proactive secret-version checks
   */
  public App(
          final String secretId, final DataSourceFactoryProvider factory, final long refreshIntervalSeconds) {
    this.client =
        new DbClient(
            RotatingDataSource.builder()
                .secretId(secretId)
                .factory(factory)
                .refreshIntervalSeconds(refreshIntervalSeconds)
                .build());
  }

  /**
   * Entry point. Builds an App using the Apache DBCP2 factory from Pool and prints the DB time.
   *
   * @param args CLI args (unused)
   * @throws Exception on unexpected failures
   */
  public static void main(String[] args) throws Exception {

    final var logger = System.getLogger(App.class.getName());

    final var app = new App("mydb/secret", Pool.c3p0Factory);
    final var result = app.getString();

    logger.log(DEBUG, "DB Time = %s".formatted(result));
  }

  /**
   * Queries the database for the current time using the reusable rotating data source and retry
   * logic.
   *
   * @return the time string returned by the database
   * @throws SQLException if the query fails
   */
  public String getString() throws SQLException {
    return client.executeWithRetry(
        conn -> {
          try (final var stmt = conn.createStatement();
              var rs = stmt.executeQuery("SELECT NOW()")) {
            rs.next();
            return rs.getString(1);
          }
        },
        Retry.Policy.fixed(2, 1_000));
  }

  /**
   * Shuts down resources associated with this App, notably closing the underlying rotating data
   * source if it supports proactive refresh or pooling.
   */
  public void shutdown() {
    try {
      client.rotatingDataSource().shutdown();
    } catch (final Exception ignored) {
    }
  }
}
