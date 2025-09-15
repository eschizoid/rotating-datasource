package com.example;

import com.example.smrotator.core.DataSourceFactory;
import com.example.smrotator.core.Retry;
import com.example.smrotator.core.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/** Spring Data example: expose RotatingDataSource as a DataSource bean so repositories use it. */
@SpringBootApplication
public class Main {
  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }

  // Functional service-layer bean: run ORM operations with auth-retry.
  @FunctionalInterface
  public interface AuthRetry {
    <T> T run(java.util.function.Supplier<T> op);
  }

  @Bean
  public DataSourceFactory dataSourceFactory() {
    return secret -> {
      final var cfg = new HikariConfig();
      cfg.setJdbcUrl(
          "jdbc:%s://%s:%d/%s"
              .formatted(secret.engine(), secret.host(), secret.port(), secret.dbname()));
      cfg.setUsername(secret.username());
      cfg.setPassword(secret.password());
      cfg.setMaximumPoolSize(5);
      cfg.setPoolName("spring-data-rotating-pool");
      return new HikariDataSource(cfg);
    };
  }

  @Bean
  public RotatingDataSource rotatingDataSource(final DataSourceFactory factory) {
    final var secretId = System.getProperty("db.secretId");
    return new RotatingDataSource(secretId, factory);
  }

  @Bean
  public DataSource dataSource(final RotatingDataSource rotating) {
    return rotating.getDataSource();
  }

  @Bean
  public AuthRetry authRetry(final RotatingDataSource rotating) {
    return new AuthRetry() {
      @Override
      public <T> T run(final java.util.function.Supplier<T> op) {
        return Retry.authRetry(op, rotating);
      }
    };
  }
}

  // Your Spring Data repositories will automatically use the DataSource bean above.
  // If you hit an auth error at runtime, you can implement a service-layer retry that calls
  // rotatingDataSource.reset() and retries once.
