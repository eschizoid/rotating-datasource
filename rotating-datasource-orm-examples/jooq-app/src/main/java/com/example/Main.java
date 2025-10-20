package com.example;

import com.example.rotating.datasource.core.jdbc.DataSourceFactoryProvider;
import com.example.rotating.datasource.core.jdbc.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/** jOOQ example: wiring RotatingDataSource into a DSLContext. */
public class Main {
  private static volatile RotatingDataSource INSTANCE;

  public static void main(String[] args) {
    final var secretId = System.getProperty("db.secretId");
    rotatingDataSource(secretId);
  }

  public static synchronized RotatingDataSource rotatingDataSource(final String secretId) {
    if (INSTANCE == null) {
      INSTANCE =
          RotatingDataSource.builder()
              .secretId(secretId)
              .factory(hikariFactory())
              .refreshIntervalSeconds(0L)
              .build();
      Runtime.getRuntime().addShutdownHook(new Thread(Main::shutdownRotating));
    }
    return INSTANCE;
  }

  public static synchronized void shutdownRotating() {
    if (INSTANCE != null) {
      try {
        INSTANCE.shutdown();
      } catch (final Exception ignored) {
      } finally {
        INSTANCE = null;
      }
    }
  }

  public static DataSourceFactoryProvider hikariFactory() {
    return secret -> {
      final var cfg = new HikariConfig();
      cfg.setJdbcUrl(
          "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname()));
      cfg.setUsername(secret.username());
      cfg.setPassword(secret.password());
      cfg.setMaximumPoolSize(5);
      cfg.setPoolName("jooq-rotating-pool");
      return new HikariDataSource(cfg);
    };
  }

  public static DSLContext buildDsl(final RotatingDataSource rotating) {
    return DSL.using(rotating, SQLDialect.POSTGRES);
  }
}
