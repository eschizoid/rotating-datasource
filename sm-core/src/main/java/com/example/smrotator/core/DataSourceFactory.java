package com.example.smrotator.core;

import javax.sql.DataSource;

/**
 * Functional factory that creates a {@link DataSource} from a {@link DbSecret}. Implementations
 * typically configure a connection pool using values from the secret.
 */
@FunctionalInterface
public interface DataSourceFactory {
  /**
   * Creates a new {@link DataSource} configured for the provided secret.
   *
   * @param secret the database secret containing connection parameters
   * @return a new {@link DataSource}
   */
  DataSource create(final DbSecret secret);
}
