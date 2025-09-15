package com.example;

import static com.example.smrotator.core.Retry.authRetry;

import com.example.smrotator.core.DataSourceFactory;
import com.example.smrotator.core.RotatingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/** jOOQ example: wiring RotatingDataSource into a DSLContext. */
public class Main {
  public static void main(String[] args) {
    final var secretId = System.getProperty("db.secretId");
    final var rotating = new RotatingDataSource(secretId, hikariFactory(), 10);

    final var result = authRetry(() -> buildDsl(rotating).fetchOne("SELECT now()"), rotating);

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
      cfg.setPoolName("jooq-rotating-pool");
      return new HikariDataSource(cfg);
    };
  }

  static DSLContext buildDsl(final RotatingDataSource rotating) {
    return DSL.using(rotating.getDataSource(), SQLDialect.POSTGRES);
  }
}
