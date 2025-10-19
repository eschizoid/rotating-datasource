package com.example.rotatingdatasource.core.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Thin database client that executes operations using a {@link RotatingDataSource} and retries once
 * on authentication failures after rotating credentials.
 *
 * @param rotatingDataSource data source wrapper that can rotate underlying credentials
 */
public record DbClient(RotatingDataSource rotatingDataSource) {

  /**
   * Executes a database operation, retrying once if the first attempt fails due to an
   * authentication-related {@link SQLException}. On retry, the {@link RotatingDataSource} is reset
   * to pick up fresh credentials.
   *
   * @param operation the database operation to run with an open {@link Connection}
   * @param policy retry policy configuration
   * @param <T> the operation result type
   * @return the value returned by the operation
   * @throws SQLException if the operation fails (after retry if applicable)
   */
  public <T> T executeWithRetry(final DbOperation<T> operation, final Retry.Policy policy)
      throws SQLException {
    return Retry.onException(
        () -> run(operation), Retry::isAuthError, rotatingDataSource::reset, policy);
  }

  /**
   * Opens a connection and executes the provided operation.
   *
   * @param operation unit of work using an open {@link Connection}
   * @param <T> return type
   * @return operation result
   * @throws SQLException if opening the connection or executing the work fails
   */
  private <T> T run(final DbOperation<T> operation) throws SQLException {
    try (final var conn = rotatingDataSource.getConnection()) {
      return operation.execute(conn);
    }
  }

  /**
   * Database operation executed against an open {@link Connection}.
   *
   * @param <T> result type
   */
  @FunctionalInterface
  public interface DbOperation<T> {
    /**
     * Executes the operation with the provided connection.
     *
     * @param conn an open JDBC connection
     * @return operation result
     * @throws SQLException on database errors
     */
    T execute(final Connection conn) throws SQLException;
  }
}
