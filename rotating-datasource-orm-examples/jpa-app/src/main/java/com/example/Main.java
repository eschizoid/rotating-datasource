package com.example;

import com.example.rotatingdatasource.core.jdbc.DataSourceFactoryProvider;
import com.example.rotatingdatasource.core.jdbc.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.util.HashMap;

/** JPA example: wiring RotatingDataSource into an EntityManagerFactory. */
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

  static DataSourceFactoryProvider hikariFactory() {
    return secret -> {
      final var cfg = new HikariConfig();
      cfg.setJdbcUrl(
          "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname()));
      cfg.setUsername(secret.username());
      cfg.setPassword(secret.password());
      cfg.setMaximumPoolSize(5);
      cfg.setPoolName("jpa-rotating-pool");
      return new HikariDataSource(cfg);
    };
  }

  static EntityManagerFactory buildEmf(final RotatingDataSource rotating) {
    final var props = new HashMap<String, Object>();
    props.put("jakarta.persistence.nonJtaDataSource", rotating);
    props.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
    props.put("hibernate.hbm2ddl.auto", "none");
    return Persistence.createEntityManagerFactory("examplePU", props);
  }
}
