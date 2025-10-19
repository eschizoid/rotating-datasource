package com.example;

import com.example.rotatingdatasource.core.DataSourceFactoryProvider;
import org.apache.commons.dbcp2.BasicDataSource;

public class Pool {

  /** Public factory for Apache DBCP2 DataSource instances built from a DbSecret. */
  public static final DataSourceFactoryProvider dbcpFactory =
      secret -> {
        final var ds = new BasicDataSource();
        final var jdbcUrl =
            "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname());
        ds.setUrl(jdbcUrl);
        ds.setUsername(secret.username());
        ds.setPassword(secret.password());
        ds.setMaxTotal(10);
        return ds;
      };
}
