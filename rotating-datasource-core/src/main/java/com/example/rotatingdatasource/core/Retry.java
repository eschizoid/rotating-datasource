package com.example.rotatingdatasource.core;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Internal retry helper for {@link RotatingDataSource}.
 *
 * <p>Provides retry logic for authentication failures and transient connection errors during
 * credential rotation.
 *
 * <p><strong>This class is package-private and not intended for application use.</strong> {@link
 * RotatingDataSource} handles all retry logic automatically.
 */
final class Retry {

  private static final System.Logger LOGGER = System.getLogger(Retry.class.getName());

  private Retry() {}

  private static final String[] AUTH_KEYWORDS =
      new String[] {
        "access denied",
        "authentication failed",
        "password authentication failed",
        "invalid password",
        "permission denied"
      };

  private static final String[] TRANSIENT_CONN_KEYWORDS =
      new String[] {
        "connection refused",
        "connection reset",
        "i/o error",
        "socket closed",
        "broken pipe",
        "pool",
        "closed",
        "timeout"
      };

  /**
   * Supplier that can throw {@link SQLException}.
   *
   * @param <T> result type
   */
  @FunctionalInterface
  interface SqlExceptionSupplier<T, E extends SQLException> {
    T get() throws SQLException;
  }

  /**
   * Retry policy configuration supporting exponential backoff.
   *
   * @param maxAttempts maximum number of attempts (including first), must be >= 1
   * @param initialDelayMillis initial delay between retries in milliseconds, must be >= 0
   * @param maxDelayMillis maximum delay cap for exponential backoff, must be >= initialDelayMillis
   * @param backoffMultiplier multiplier for exponential backoff (1.0 = fixed delay), must be >= 1.0
   * @param jitter whether to add random jitter (up to 25%) to delays
   */
  record Policy(
      int maxAttempts,
      long initialDelayMillis,
      long maxDelayMillis,
      double backoffMultiplier,
      boolean jitter) {

    Policy {
      if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
      if (initialDelayMillis < 0)
        throw new IllegalArgumentException("initialDelayMillis must be >= 0");
      if (maxDelayMillis < initialDelayMillis)
        throw new IllegalArgumentException("maxDelayMillis must be >= initialDelayMillis");
      if (backoffMultiplier < 1.0)
        throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
    }

    /**
     * Creates a fixed delay retry policy.
     *
     * @param attempts number of attempts (including first)
     * @param delayMillis delay between attempts in milliseconds
     * @return fixed delay retry policy
     */
    static Policy fixed(final int attempts, final long delayMillis) {
      return new Policy(attempts, delayMillis, delayMillis, 1.0, false);
    }

    /**
     * Creates an exponential backoff retry policy with jitter.
     *
     * <p>Delays grow exponentially (2x) up to a maximum of 45 seconds, with 25% random jitter.
     *
     * @param attempts number of attempts (including first)
     * @param initialDelay initial delay in milliseconds
     * @return exponential backoff retry policy with jitter
     */
    static Policy exponential(final int attempts, final long initialDelay) {
      return new Policy(attempts, initialDelay, 60_000L, 2.0, true);
    }

    /**
     * Calculates the delay for a given attempt number.
     *
     * @param attempt current attempt number (1-based)
     * @return delay in milliseconds before the next attempt
     */
    long calculateDelay(final int attempt) {
      if (attempt <= 1) return 0L;

      var delay = initialDelayMillis;
      if (backoffMultiplier > 1.0) {
        delay = (long) (initialDelayMillis * Math.pow(backoffMultiplier, attempt - 2));
        delay = Math.min(delay, maxDelayMillis);
      }

      if (jitter) {
        final var jitterAmount = (long) (delay * 0.25 * Math.random());
        delay += jitterAmount;
      }

      return delay;
    }
  }

  /** Authentication error detector for identifying auth failures in SQLExceptions. */
  @FunctionalInterface
  interface AuthErrorDetector {
    /**
     * Determines if the exception represents an authentication error.
     *
     * @param e the exception to check
     * @return true if it's an authentication error
     */
    boolean isAuthError(final SQLException e);

    /**
     * Returns the default detector using built-in heuristics.
     *
     * <p>Checks SQLState codes (28000, 28P01) and common auth error message patterns.
     *
     * @return default detector
     */
    static AuthErrorDetector defaultDetector() {
      return Retry::isAuthError;
    }

    /**
     * Creates a custom detector from a predicate.
     *
     * @param predicate the predicate to use for detection
     * @return custom detector
     */
    static AuthErrorDetector custom(final Predicate<SQLException> predicate) {
      return predicate::test;
    }

    /**
     * Combines this detector with another using OR logic.
     *
     * @param other the other detector to combine with
     * @return combined detector
     */
    default AuthErrorDetector or(final AuthErrorDetector other) {
      return e -> this.isAuthError(e) || other.isAuthError(e);
    }
  }

  /**
   * Retries supplier on transient connection errors using the provided policy.
   *
   * <p>Internal method used by {@link RotatingDataSource#getConnection()}.
   *
   * @param op operation to execute
   * @param policy retry policy configuration
   * @param <T> result type
   * @return operation result
   * @throws RuntimeException if all attempts fail
   */
  static <T> T transientRetry(final Supplier<? extends T> op, final Policy policy) {
    int attempt = 0;
    while (true) {
      attempt++;
      try {
        return op.get();
      } catch (final RuntimeException e) {
        final var sqlException = findSqlException(e);
        if (!isTransientConnectionError(sqlException)) throw e;

        if (attempt >= policy.maxAttempts()) {
          LOGGER.log(WARNING, "All {0} transient retry attempts failed", attempt);
          throw e;
        }

        LOGGER.log(DEBUG, "Transient error on attempt {0}, retrying...", attempt);

        final var delay = policy.calculateDelay(attempt);
        if (delay > 0) {
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry delay", ie);
          }
        }
      }
    }
  }

  /**
   * Retries supplier on auth errors, calling reset hook before retry.
   *
   * <p>Internal method used by {@link RotatingDataSource#getConnection()}.
   *
   * @param op operation to execute
   * @param reset hook to call before retry (typically credential refresh)
   * @param detector auth error detector
   * @param <T> result type
   * @return operation result
   * @throws RuntimeException if auth error persists after reset or non-auth error occurs
   */
  static <T> T authRetry(
      final SqlExceptionSupplier<T, ?> op, final Runnable reset, final AuthErrorDetector detector) {
    try {
      return op.get();
    } catch (final SQLException e) {
      if (!detector.isAuthError(e)) {
        throw new RuntimeException(e);
      }

      LOGGER.log(WARNING, "Authentication error detected, resetting credentials");
      reset.run();

      try {
        return op.get();
      } catch (final SQLException retryException) {
        LOGGER.log(WARNING, "Retry after credential reset failed");
        throw new RuntimeException(retryException);
      }
    }
  }

  /**
   * Executes the supplier up to {@code maxAttempts} times with fixed delay retry.
   *
   * <p>When an exception is thrown and {@code shouldRetry} returns true, runs {@code beforeRetry}
   * hook and retries after the specified delay.
   *
   * <h3>Retry on Connection Failures</h3>
   *
   * <pre>{@code
   * String result = Retry.onException(
   *     () -> jdbcTemplate.queryForObject("SELECT value FROM config", String.class),
   *     e -> e.getSQLState().equals("08003"), // connection failure
   *     () -> System.out.println("Connection lost, retrying..."),
   *     3,    // 3 attempts total
   *     500L  // 500ms delay between attempts
   * );
   * }</pre>
   *
   * <h3>Retry on Serialization Failures</h3>
   *
   * <pre>{@code
   * final var count = Retry.onException(
   *     () -> {
   *         try (var stmt = connection.createStatement()) {
   *             var rs = stmt.executeQuery("SELECT COUNT(*) FROM users");
   *             return rs.next() ? rs.getInt(1) : 0;
   *         }
   *     },
   *     e -> e.getSQLState().startsWith("40"), // serialization failures
   *     () -> {},
   *     5,     // up to 5 attempts
   *     100L   // 100ms between attempts
   * );
   * }</pre>
   *
   * @param supplier operation to execute
   * @param shouldRetry predicate determining whether the exception is retryable
   * @param beforeRetry hook to run before each retry attempt (not before first attempt)
   * @param maxAttempts total attempts including the first (must be >= 1)
   * @param delayMillis delay between attempts in milliseconds (non-negative)
   * @param <T> result type
   * @param <E> exception type extending SQLException
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
    var attempt = 0;

    while (true) {
      attempt++;
      try {
        return supplier.get();
      } catch (final SQLException ex) {
        @SuppressWarnings("unchecked")
        final E typedException = (E) ex;

        if (!shouldRetry.test(typedException) || attempt >= maxAttempts) {
          throw typedException;
        }

        LOGGER.log(DEBUG, "Attempt {0} failed, retrying...", attempt);
        beforeRetry.run();

        if (delayMillis > 0) {
          try {
            Thread.sleep(delayMillis);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw typedException;
          }
        }
      }
    }
  }

  /**
   * Finds the first SQLException in a throwable cause chain.
   *
   * @param t the throwable to search
   * @return the first SQLException, or null if none found
   */
  static SQLException findSqlException(final Throwable t) {
    Throwable cur = t;
    SQLException last = null;
    while (cur != null) {
      if (cur instanceof SQLException sql) {
        if (last == null) last = sql;
        SQLException next = sql.getNextException();
        while (next != null) {
          last = next;
          next = next.getNextException();
        }
      }
      cur = cur.getCause();
    }
    return last;
  }

  /**
   * Detects authentication errors based on SQLState and message patterns.
   *
   * <p>Checks for SQLState codes 28000, 28P01 and common auth error keywords.
   *
   * @param e the exception to check
   * @return true if authentication error
   */
  static boolean isAuthError(final SQLException e) {
    if (e == null) return false;

    final var state = e.getSQLState();
    final var isAuthState = "28000".equals(state) || "28P01".equals(state);

    final var msg = e.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (final var keyword : AUTH_KEYWORDS) if (lower.contains(keyword)) return true;
    }

    return isAuthState;
  }

  /**
   * Detects transient connection errors likely to recover on retry.
   *
   * <p>Checks for SQLState class 08, SQLTransientException, and transient error keywords.
   *
   * @param e the exception to check
   * @return true if transient connection error
   */
  static boolean isTransientConnectionError(final SQLException e) {
    if (e == null) return false;

    final var state = e.getSQLState();
    if (state != null && state.startsWith("08")) return true;

    if (e instanceof SQLTransientException) return true;

    if (e.getClass().getSimpleName().contains("JDBCConnectionException")) return true;

    final var msg = e.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (final var keyword : TRANSIENT_CONN_KEYWORDS) if (lower.contains(keyword)) return true;
    }

    return false;
  }
}
