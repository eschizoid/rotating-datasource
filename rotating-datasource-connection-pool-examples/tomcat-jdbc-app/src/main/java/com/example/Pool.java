package com.example;

import com.example.rotating.datasource.core.jdbc.DataSourceFactoryProvider;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

public class Pool {

  /** Public factory for Tomcat JDBC Pool DataSource instances built from a DbSecret. */
  public static final DataSourceFactoryProvider tomcatFactory =
      secret -> {
        final var p = new PoolProperties();
        p.setUrl(
            "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname()));
        p.setDriverClassName("org.postgresql.Driver");
        p.setUsername(secret.username());
        p.setPassword(secret.password());
        p.setMaxActive(10);
        return new DataSource(p);
      };
}
