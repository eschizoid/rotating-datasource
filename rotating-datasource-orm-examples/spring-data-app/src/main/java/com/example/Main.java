package com.example;

import com.example.rotatingdatasource.core.DataSourceFactoryProvider;
import com.example.rotatingdatasource.core.RotatingDataSource;
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

  @Bean
  public DataSourceFactoryProvider dataSourceFactory() {
    return secret -> {
      final var cfg = new HikariConfig();
      cfg.setJdbcUrl(
          "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname()));
      cfg.setUsername(secret.username());
      cfg.setPassword(secret.password());
      cfg.setMaximumPoolSize(5);
      cfg.setPoolName("spring-data-rotating-pool");
      return new HikariDataSource(cfg);
    };
  }

  @Bean
  public DataSource dataSource(final DataSourceFactoryProvider factory) {
    final var secretId = System.getProperty("db.secretId");
    return RotatingDataSource.builder()
        .secretId(secretId)
        .factory(factory)
        .refreshIntervalSeconds(60)
        .build();
  }
}
