package com.example;

import com.example.rotatingdatasource.core.DataSourceFactory;
import com.example.rotatingdatasource.core.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

/** Hibernate example: wiring RotatingDataSource into a SessionFactory. */
public class Main {
  private static volatile RotatingDataSource INSTANCE;

  public static void main(String[] args) {
    final var secretId = System.getProperty("db.secretId");
    rotatingDataSource(secretId);
  }

  static SessionFactory buildSessionFactory(final RotatingDataSource rotating) {
    final var hibernateCfg = new Configuration();
    hibernateCfg.setProperty("hibernate.hikari.maximumPoolSize", "100");
    hibernateCfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
    hibernateCfg.setProperty("hibernate.hbm2ddl.auto", "none");

    final var serviceRegistry =
        new StandardServiceRegistryBuilder()
            .applySetting("hibernate.connection.datasource", rotating)
            .applySettings(hibernateCfg.getProperties())
            .build();

    return hibernateCfg.buildSessionFactory(serviceRegistry);
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

  static DataSourceFactory hikariFactory() {
    return secret -> {
      final var cfg = new HikariConfig();
      cfg.setJdbcUrl(
          "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname()));
      cfg.setUsername(secret.username());
      cfg.setPassword(secret.password());
      cfg.setMaximumPoolSize(5);
      cfg.setPoolName("hibernate-rotating-pool");
      return new HikariDataSource(cfg);
    };
  }
}
