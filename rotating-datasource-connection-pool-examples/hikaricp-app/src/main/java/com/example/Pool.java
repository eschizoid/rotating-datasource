package com.example;

import com.example.rotatingdatasource.core.DataSourceFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Pool {

  /** Public factory for HikariCP DataSource instances built from a DbSecret. */
  public static final DataSourceFactory hikariFactory =
      secret -> {
        final var jdbcUrl =
            String.format(
                "jdbc:postgresql://%s:%d/%s", secret.host(), secret.port(), secret.dbname());
        final var config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(secret.username());
        config.setPassword(secret.password());
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
      };
}
