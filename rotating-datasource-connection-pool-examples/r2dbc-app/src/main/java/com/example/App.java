package com.example;

import static java.lang.System.Logger.Level.DEBUG;

import com.example.rotatingdatasource.core.reactive.ConnectionFactoryProvider;
import com.example.rotatingdatasource.core.reactive.RotatingConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

/** Demo application showing how to query a database while surviving secret rotation using R2DBC. */
public class App {
  private final ConnectionFactory connectionFactory;

  /**
   * Constructs the application with a single RotatingConnectionFactory instance that will be reused
   * for all later calls to {@link #getString()}.
   *
   * @param secretId the secret identifier to load from AWS Secrets Manager
   * @param factory a factory to create ConnectionFactory instances from secrets
   */
  public App(final String secretId, final ConnectionFactoryProvider factory) {
    this(secretId, factory, 0L);
  }

  /**
   * Constructs the application using a RotatingConnectionFactory that can optionally perform
   * proactive refresh checks at a fixed interval.
   *
   * @param secretId the secret identifier to load from AWS Secrets Manager
   * @param factory a factory to create ConnectionFactories from secrets
   * @param refreshIntervalSeconds if > 0, enables proactive secret-version checks
   */
  public App(
      final String secretId,
      final ConnectionFactoryProvider factory,
      final long refreshIntervalSeconds) {
    this.connectionFactory =
        RotatingConnectionFactory.builder()
            .secretId(secretId)
            .factory(factory)
            .refreshIntervalSeconds(refreshIntervalSeconds)
            .build();
  }

  /**
   * Entry point. Builds an App using the R2DBC factory from Pool and prints the DB time.
   *
   * @param args CLI args (unused)
   * @throws Exception on unexpected failures
   */
  public static void main(String[] args) throws Exception {
    final var logger = System.getLogger(App.class.getName());

    final var app = new App("mydb/secret", Pool.r2dbcPoolFactory);
    final var result = app.getString();

    logger.log(DEBUG, "DB Time = %s".formatted(result));
  }

  /**
   * Queries the database for the current time using the reusable rotating connection factory.
   *
   * @return the time string returned by the database
   */
  public String getString() {
    return Mono.usingWhen(
            Mono.from(connectionFactory.create()),
            conn ->
                Mono.from(conn.createStatement("SELECT NOW()").execute())
                    .flatMap(result -> Mono.from(result.map((row, md) -> row.get(0, String.class))))
                    .onErrorResume(Mono::error),
            conn -> Mono.from(conn.close()))
        .block();
  }

  /**
   * Shuts down resources associated with this App, notably closing the underlying rotating
   * connection factory if it supports proactive refresh or pooling.
   */
  public void shutdown() {
    try {
      if (connectionFactory instanceof RotatingConnectionFactory rcf) {
        rcf.shutdown().blockOptional();
      }
    } catch (final Exception ignored) {
    }
  }
}
