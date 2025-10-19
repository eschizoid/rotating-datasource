package com.example.rotatingdatasource.core.reactive;

import com.example.rotatingdatasource.core.DbSecret;
import io.r2dbc.spi.ConnectionFactory;

/**
 * Factory that creates a {@link ConnectionFactory} from a {@link DbSecret}. Implementations
 * typically configure a reactive connection factory using values from the secret.
 */
@FunctionalInterface
public interface ConnectionFactoryProvider {
  /**
   * Creates a new {@link ConnectionFactory} configured for the provided secret.
   *
   * @param secret the database secret containing connection parameters
   * @return a new {@link ConnectionFactory}
   */
  ConnectionFactory create(final DbSecret secret);
}
