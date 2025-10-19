package com.example;

import com.example.rotating.datasource.core.jdbc.DataSourceFactoryProvider;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class Pool {

  /** Public factory for C3P0 DataSource instances built from a DbSecret. */
  public static final DataSourceFactoryProvider c3p0Factory =
      secret -> {
        final var ds = new ComboPooledDataSource();
        final var jdbcUrl =
            "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname());
        ds.setJdbcUrl(jdbcUrl);
        ds.setUser(secret.username());
        ds.setPassword(secret.password());
        ds.setMaxPoolSize(10);
        return ds;
      };
}
