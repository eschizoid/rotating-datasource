package com.example;

import com.example.rotatingdatasource.core.ConnectionFactoryProvider;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import java.time.Duration;

public class Pool {

  public static final ConnectionFactoryProvider r2dbcPoolFactory =
      secret -> {
        final var config =
            PostgresqlConnectionConfiguration.builder()
                .host(secret.host())
                .port(secret.port())
                .database(secret.dbname())
                .username(secret.username())
                .password(secret.password())
                .build();

        final var connectionFactory = new PostgresqlConnectionFactory(config);

        final var poolConfig =
            ConnectionPoolConfiguration.builder(connectionFactory)
                .initialSize(5)
                .maxSize(10)
                .maxIdleTime(Duration.ofMinutes(30))
                .maxAcquireTime(Duration.ofSeconds(3))
                .build();

        return new ConnectionPool(poolConfig);
      };
}
