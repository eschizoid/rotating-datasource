package com.example;

import static com.example.smrotator.core.Retry.authRetry;

import com.example.smrotator.core.DataSourceFactory;
import com.example.smrotator.core.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.util.HashMap;

/** JPA example: wiring RotatingDataSource into an EntityManagerFactory. */
public class Main {
  public static void main(String[] args) {
    final var secretId = System.getProperty("db.secretId");
    final var rotating = new RotatingDataSource(secretId, hikariFactory(), 10);

    final var result =
        authRetry(
            () -> {
              try (var emf = buildEmf(rotating);
                  var em = emf.createEntityManager()) {
                return em.createNativeQuery("select now()").getSingleResult();
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
      cfg.setPoolName("jpa-rotating-pool");
      return new HikariDataSource(cfg);
    };
  }

  static EntityManagerFactory buildEmf(final RotatingDataSource rotating) {
    final var props = new HashMap<String, Object>();
    props.put("jakarta.persistence.nonJtaDataSource", rotating.getDataSource());
    props.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
    props.put("hibernate.hbm2ddl.auto", "none");

    // The persistence unit name should match your META-INF/persistence.xml
    return Persistence.createEntityManagerFactory("examplePU", props);
  }
}
