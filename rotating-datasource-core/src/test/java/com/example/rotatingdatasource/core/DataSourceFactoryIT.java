package com.example.rotatingdatasource.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.stream.Stream;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@DisabledIfSystemProperty(named = "tests.integration.disable", matches = "true")
public class DataSourceFactoryIT {

  enum DbPlatform {
    POSTGRES {
      @Override
      JdbcDatabaseContainer<?> newContainer() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"));
      }

      @Override
      String jdbcUrl(final DbSecret s) {
        return "jdbc:postgresql://%s:%d/%s".formatted(s.host(), s.port(), s.dbname());
      }

      @Override
      String engine() {
        return "postgres";
      }
    },
    MYSQL {
      @Override
      JdbcDatabaseContainer<?> newContainer() {
        return new MySQLContainer<>(DockerImageName.parse("mysql:8"));
      }

      @Override
      String jdbcUrl(final DbSecret s) {
        return "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true"
            .formatted(s.host(), s.port(), s.dbname());
      }

      @Override
      String engine() {
        return "mysql";
      }
    };

    abstract JdbcDatabaseContainer<?> newContainer();

    abstract String jdbcUrl(final DbSecret s);

    abstract String engine();
  }

  static Stream<Arguments> platforms() {
    return Stream.of(Arguments.of(DbPlatform.POSTGRES), Arguments.of(DbPlatform.MYSQL));
  }

  @ParameterizedTest
  @MethodSource("platforms")
  void factoryBuildsWorkingDataSourceAgainst(final DbPlatform platform) throws Exception {
    assumeTrue(dockerAvailable(), "Docker not available, skipping test");

    try (JdbcDatabaseContainer<?> container = platform.newContainer()) {
      container.start();

      final var secret =
          new DbSecret(
              container.getUsername(),
              container.getPassword(),
              platform.engine(),
              container.getHost(),
              container.getFirstMappedPort(),
              container.getDatabaseName());

      final DataSourceFactory factory =
          s -> {
            final var cfg = new HikariConfig();
            cfg.setJdbcUrl(platform.jdbcUrl(s));
            cfg.setUsername(s.username());
            cfg.setPassword(s.password());
            cfg.setMaximumPoolSize(2);
            cfg.setPoolName("dsf-test-pool-" + platform.name().toLowerCase());
            return new HikariDataSource(cfg);
          };

      final var ds = factory.create(secret);
      try (var conn = ds.getConnection();
          final var st = conn.createStatement();
          final var rs = st.executeQuery("SELECT 1")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      } finally {
        final var ac = (AutoCloseable) ds;
        try {
          ac.close();
        } catch (final Exception ignored) {
        }
      }
    }
  }

  private boolean dockerAvailable() {
    try {
      DockerClientFactory.instance().client();
      return true;
    } catch (final Throwable t) {
      return false;
    }
  }
}
