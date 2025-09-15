package com.example;

import static com.example.smrotator.core.Retry.authRetry;

import com.example.smrotator.core.DataSourceFactory;
import com.example.smrotator.core.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

/** Hibernate example: wiring RotatingDataSource into a SessionFactory. */
public class Main {
  public static void main(String[] args) {
    final var secretId = System.getProperty("db.secretId");
    final var rotating = new RotatingDataSource(secretId, hikariFactory(), 10);

    final var result =
        authRetry(
            () -> {
              try (final var sessionFactory = buildSessionFactory(rotating);
                  final var session = sessionFactory.openSession()) {
                return session.createNativeQuery("select now()").getSingleResult();
              }
            },
            rotating);

    System.out.println(result);
  }

  static DataSourceFactory hikariFactory() {
    return secret -> {
      final var cfg = new HikariConfig();
      cfg.setJdbcUrl(
          "jdbc:%s://%s:%d/%s"
              .formatted(secret.engine(), secret.host(), secret.port(), secret.dbname()));
      cfg.setUsername(secret.username());
      cfg.setPassword(secret.password());
      cfg.setMaximumPoolSize(5);
      cfg.setPoolName("hibernate-rotating-pool");
      return new HikariDataSource(cfg);
    };
  }

  static SessionFactory buildSessionFactory(final RotatingDataSource rotating) {
    final var hibernateCfg = new Configuration();
    hibernateCfg.setProperty("hibernate.hikari.maximumPoolSize", "5");
    hibernateCfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
    hibernateCfg.setProperty("hibernate.hbm2ddl.auto", "none");

    final var serviceRegistry =
        new StandardServiceRegistryBuilder()
            .applySetting("hibernate.connection.datasource", rotating.getDataSource())
            .applySettings(hibernateCfg.getProperties())
            .build();

    return hibernateCfg.buildSessionFactory(serviceRegistry);
  }
}
