package com.example.smrotator.core;

import java.sql.SQLException;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Small functional retry helper to express retry-on-exception declaratively. */
public final class Retry {

  private Retry() {}

  // Lower-cased substrings that commonly indicate authentication problems in JDBC drivers
  private static final String[] AUTH_KEYWORDS =
      new String[] {
        "access denied",
        "authentication failed",
        "password authentication failed",
        "invalid password",
        "permission denied"
      };

  /**
   * A supplier that can throw a {@link SQLException} exception.
   *
   * @param <T> result type
   * @param <E> exception type
   */
  @FunctionalInterface
  public interface SqlExceptionSupplier<T, E extends SQLException> {
    /**
     * Supplies a value or throws a {@link SQLException}.
     *
     * @return supplied value
     * @throws E on failure
     */
    T get() throws E;
  }

  /**
   * Executes the supplier up to {@code maxAttempts} times. When an exception is thrown and {@code
   * shouldRetry} returns true, runs {@code beforeRetry} and retries after an optional fixed delay.
   *
   * @param supplier operation to execute
   * @param shouldRetry predicate determining whether the exception is retryable
   * @param beforeRetry hook to run before each retry attempt
   * @param maxAttempts total attempts including the first (must be >= 1)
   * @param delayMillis delay between attempts in milliseconds (non-negative)
   * @param <T> result type
   * @param <E> exception type
   * @return the supplier result
   * @throws E if all attempts fail or the exception is not retryable
   */
  public static <T, E extends SQLException> T onException(
      final SqlExceptionSupplier<T, E> supplier,
      final Predicate<E> shouldRetry,
      final Runnable beforeRetry,
      final int maxAttempts,
      final long delayMillis)
      throws E {
    if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
    var attempt = 1;
    while (true) {
      try {
        return supplier.get();
      } catch (final Exception ex) {
        @SuppressWarnings({"unchecked"})
        final var e = (E) ex;
        if (attempt >= maxAttempts || !shouldRetry.test(e)) throw e;
        beforeRetry.run();
        if (delayMillis > 0) {
          try {
            Thread.sleep(delayMillis);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        }
        attempt++;
      }
    }
  }

  /**
   * Convenience for ORMs and higher layers: run an operation, and if it fails with a
   * RuntimeException that contains an authentication-related SQLException, reset the rotating data
   * source and retry once.
   *
   * @param <T> result type
   * @param op operation to run
   * @param rotating data source wrapper
   * @return the operation result
   * @throws RuntimeException if the operation fails after retry
   */
  public static <T> T authRetry(final Supplier<T> op, final RotatingDataSource rotating) {
    return authRetry(op, rotating::reset);
  }

  // Package-private to keep API minimal while enabling testing and potential reuse.
  static <T> T authRetry(final Supplier<T> op, final Runnable reset) {
    try {
      return op.get();
    } catch (final RuntimeException e) {
      final var sql = findSqlException(e);
      if (isAuthError(sql)) {
        reset.run();
        return op.get();
      }
      throw e;
    }
  }

  /**
   * Finds the first SQLException in a throwable cause chain, or null if none.
   *
   * @param t the throwable
   * @return the first SQLException in the chain, or null
   */
  public static SQLException findSqlException(final Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof SQLException se) return se;
      cur = cur.getCause();
    }
    return null;
  }

  /**
   * Heuristic to detect authentication errors based on SQLState and common messages.
   *
   * @param e the exception to check
   * @return true if the exception is an authentication error
   */
  public static boolean isAuthError(final SQLException e) {
    if (e == null) return false;
    final var isStateAuth = "28000".equals(e.getSQLState());
    final var msg = e.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (final var kw : AUTH_KEYWORDS) if (lower.contains(kw)) return true;
    }
    return isStateAuth;
  }
}
