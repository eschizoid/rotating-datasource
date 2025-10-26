package com.example;

import com.example.rotating.datasource.core.reactive.ConnectionFactoryProvider;
import com.example.rotating.datasource.core.reactive.RotatingConnectionFactory;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/** Spring Data Reactive example: expose RotatingConnectionFactory so R2DBC uses it. */
@SpringBootApplication
public class Main {
  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }

  @Bean
  public ConnectionFactoryProvider connectionFactoryProvider() {
    return secret -> {
      final var pgCfg =
          PostgresqlConnectionConfiguration.builder()
              .host(secret.host())
              .port(secret.port())
              .database(secret.dbname())
              .username(secret.username())
              .password(secret.password())
              .build();
      final var base = new PostgresqlConnectionFactory(pgCfg);
      final var poolCfg = ConnectionPoolConfiguration.builder(base).build();
      return new ConnectionPool(poolCfg);
    };
  }

  @Bean
  public ConnectionFactory connectionFactory(final ConnectionFactoryProvider provider) {
    final var secretId = System.getProperty("db.secretId");
    return RotatingConnectionFactory.builder()
        .secretId(secretId)
        .factory(provider)
        .refreshIntervalSeconds(60)
        .build();
  }
}
