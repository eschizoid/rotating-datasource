package com.example.rotatingdatasource.core;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Functional retry helper for declaratively expressing retry-on-exception logic.
 *
 * <p>Provides utilities for retrying SQL operations with configurable policies, auth error
 * detection, transient connection error handling, and observability hooks.
 *
 * <p><strong>Note:</strong> {@link RotatingDataSource} includes automatic retry logic for
 * connection acquisition. Direct use of this class is only needed for:
 *
 * <ul>
 *   <li>Custom retry scenarios beyond auth failures (e.g., deadlock retries)
 *   <li>Rare edge cases where credentials expire during long-running transactions
 *   <li>Application-level retry policies independent of connection acquisition
 * </ul>
 *
 * <h2>Basic Usage</h2>
 *
 * <pre>{@code
 * // Retry on serialization failures (custom scenario)
 * final var result = Retry.onException(
 *     () -> executeQuery(),
 *     e -> e.getSQLState().startsWith("40"),
 *     () -> System.out.println("Retrying..."),
 *     3, 100L
 * );
 * }</pre>
 *
 * <h2>RotatingDataSource Integration</h2>
 *
 * <p>{@link RotatingDataSource} automatically retries {@code getConnection()} on auth failures and
 * transient errors. ORM frameworks using it inherit this behavior:
 *
 * <pre>{@code
 * // No explicit retry needed - auth/transient failures handled automatically
 * var rotatingDs = new RotatingDataSource(secretId, factory);
 *
 * // Hibernate
 * var sessionFactory = new Configuration()
 *     .setProperty("hibernate.connection.datasource", rotatingDs)
 *     .buildSessionFactory();
 *
 * // Auth/transient failures during connection acquisition are retried automatically
 * try (var session = sessionFactory.openSession()) {
 *     var users = session.createQuery("FROM User", User.class).list();
 * }
 * }</pre>
 *
 * @see RotatingDataSource
 * @see Policy
 * @see AuthErrorDetector
 * @see RetryListener
 */
public final class Retry {

  private static final System.Logger LOGGER = System.getLogger(Retry.class.getName());

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

  // Lower-cased substrings that commonly indicate transient connection/pool issues
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
   * A supplier that can throw a {@link SQLException}.
   *
   * <p>Used as the functional interface for operations that need retry logic.
   *
   * <p>Example:
   *
   * <pre>{@code
   * SqlExceptionSupplier<List<User>, SQLException> supplier =
   *     () -> jdbcTemplate.query("SELECT * FROM users", rowMapper);
   *
   * final var users = Retry.onException(
   *     supplier,
   *     SQLException::isTransient,
   *     () -> {},
   *     3, 100L
   * );
   * }</pre>
   *
   * @param <T> result type
   * @param <E> exception type extending SQLException
   */
  @FunctionalInterface
  public interface SqlExceptionSupplier<T, E extends SQLException> {
    /**
     * Supplies a value or throws a {@link SQLException}.
     *
     * @return supplied value
     * @throws E on failure
     */
    T get() throws SQLException;
  }

  /**
   * Configuration for retry behavior supporting fixed delay and exponential backoff strategies.
   *
   * <p>Immutable configuration object that defines retry behavior including maximum attempts, delay
   * timing, backoff strategy, and optional jitter.
   *
   * <h3>Fixed Delay Strategy</h3>
   *
   * <p>Retries with constant delay between attempts:
   *
   * <pre>{@code
   * // Retry up to 3 times with 200ms delay between each attempt
   * var policy = Policy.fixed(3, 200L);
   *
   * final var result = Retry.onException(
   *     () -> executeQuery(),
   *     e -> e.getSQLState().startsWith("40"),
   *     () -> {},
   *     policy
   * );
   * }</pre>
   *
   * <h3>Exponential Backoff Strategy</h3>
   *
   * <p>Retries with increasing delays, useful for transient failures:
   *
   * <pre>{@code
   * // Start at 100ms, double each attempt, max 30s, with jitter
   * var policy = Policy.exponential(5, 100L);
   * // Attempt 1: immediate
   * // Attempt 2: ~100ms + jitter
   * // Attempt 3: ~200ms + jitter
   * // Attempt 4: ~400ms + jitter
   * // Attempt 5: ~800ms + jitter
   *
   * final var rotatingDs = new RotatingDataSource(secretId, factory, 0L, policy);
   * }</pre>
   *
   * <h3>Custom Strategy</h3>
   *
   * <p>Full control over all retry parameters:
   *
   * <pre>{@code
   * final var policy = new Policy(
   *     10,      // max attempts
   *     50L,     // initial delay
   *     5000L,   // max delay cap
   *     2.0,     // backoff multiplier
   *     true     // add random jitter
   * );
   * }</pre>
   *
   * @param maxAttempts maximum number of attempts (including first), must be >= 1
   * @param initialDelayMillis initial delay between retries in milliseconds, must be >= 0
   * @param maxDelayMillis maximum delay cap for exponential backoff, must be >= initialDelayMillis
   * @param backoffMultiplier multiplier for exponential backoff (1.0 = fixed delay), must be >= 1.0
   * @param jitter whether to add random jitter (up to 25%) to delays
   */
  public record Policy(
      int maxAttempts,
      long initialDelayMillis,
      long maxDelayMillis,
      double backoffMultiplier,
      boolean jitter) {

    public Policy {
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
     * <p>All retry attempts use the same delay between them.
     *
     * <p>Example:
     *
     * <pre>{@code
     * // Retry up to 5 times with 500ms between each attempt
     * var policy = Policy.fixed(5, 500L);
     *
     * var client = new DbClient(rotatingDs);
     * String result = client.executeWithRetry(conn ->
     *     conn.createStatement().executeQuery("SELECT 1")
     * );
     * }</pre>
     *
     * @param attempts number of attempts (including first)
     * @param delayMillis delay between attempts in milliseconds
     * @return fixed delay retry policy
     */
    public static Policy fixed(final int attempts, final long delayMillis) {
      return new Policy(attempts, delayMillis, delayMillis, 1.0, false);
    }

    /**
     * Creates an exponential backoff retry policy with jitter.
     *
     * <p>Delays grow exponentially (2x) up to a maximum of 30 seconds, with 25% random jitter added
     * to prevent thundering herd problems.
     *
     * <p>Example:
     *
     * <pre>{@code
     * // Retry up to 7 times, starting at 250ms
     * final var policy = Policy.exponential(7, 250L);
     *
     * // Configure RotatingDataSource with exponential backoff
     * var rotatingDs = new RotatingDataSource(
     *     "my-db-secret",
     *     secret -> createDataSource(secret),
     *     0L,      // no proactive refresh
     *     policy   // exponential backoff on auth errors
     * );
     * }</pre>
     *
     * @param attempts number of attempts (including first)
     * @param initialDelay initial delay in milliseconds
     * @return exponential backoff retry policy with jitter
     */
    public static Policy exponential(final int attempts, final long initialDelay) {
      return new Policy(attempts, initialDelay, 45_000L, 2.0, true);
    }

    /**
     * Calculates the delay for a given attempt number based on the policy configuration.
     *
     * <p>For exponential backoff, the delay is calculated as: {@code initialDelay *
     * (backoffMultiplier ^ (attempt - 2))}, capped at {@code maxDelayMillis}.
     *
     * <p>If jitter is enabled, up to 25% random variation is added.
     *
     * <p>Example:
     *
     * <pre>{@code
     * var policy = Policy.exponential(5, 100L);
     * System.out.println(policy.calculateDelay(1)); // 0 (immediate)
     * System.out.println(policy.calculateDelay(2)); // ~100ms + jitter
     * System.out.println(policy.calculateDelay(3)); // ~200ms + jitter
     * System.out.println(policy.calculateDelay(4)); // ~400ms + jitter
     * }</pre>
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
        // Add up to 25% random jitter
        final var jitterAmount = (long) (delay * 0.25 * Math.random());
        delay += jitterAmount;
      }

      return delay;
    }
  }

  /**
   * Listener for retry events, useful for observability, metrics collection, and debugging.
   *
   * <p>Notified before each retry attempt, allowing you to log, record metrics, or take other
   * actions.
   *
   * <h3>Logging Example</h3>
   *
   * <pre>{@code
   * var listener = RetryListener.logging();
   *
   * final var result = Retry.onException(
   *     () -> executeQuery(),
   *     SQLException::isTransient,
   *     () -> {},
   *     3, 100L,
   *     listener
   * );
   * }</pre>
   *
   * <h3>Metrics Example</h3>
   *
   * <pre>{@code
   * final var listener = (attempt, exception) -> {
   *     metrics.increment("db.retries",
   *         "attempt", String.valueOf(attempt),
   *         "error_type", exception.getClass().getSimpleName()
   *     );
   * };
   *
   * Retry.onException(
   *     () -> executeQuery(),
   *     e -> e.getSQLState().startsWith("40"),
   *     () -> {},
   *     5, 200L,
   *     listener
   * );
   * }</pre>
   *
   * <h3>Custom Alerting Example</h3>
   *
   * <pre>{@code
   * final var listener = (attempt, exception) -> {
   *     if (attempt >= 3) {
   *         alerting.sendAlert("High retry count: " + attempt);
   *     }
   * };
   * }</pre>
   */
  @FunctionalInterface
  public interface RetryListener {
    /**
     * Called before each retry attempt (but not before the first attempt).
     *
     * @param attempt attempt number (1-based) that just failed
     * @param exception the exception that triggered the retry
     */
    void onRetry(final int attempt, final Throwable exception);

    /**
     * Returns a no-op listener that does nothing.
     *
     * <p>Useful as a default when you don't need retry observability.
     *
     * <p>Example:
     *
     * <pre>{@code
     * var listener = RetryListener.noOp();
     * }</pre>
     *
     * @return no-op listener
     */
    static RetryListener noOp() {
      return (attempt, exception) -> {};
    }

    /**
     * Returns a listener that logs retry attempts at WARNING level.
     *
     * <p>Logs include the attempt number and exception message.
     *
     * <p>Example output:
     *
     * <pre>
     * WARNING: Retry attempt 2 after exception: Connection refused
     * </pre>
     *
     * <p>Example:
     *
     * <pre>{@code
     * final var listener = RetryListener.logging();
     *
     * Retry.onException(
     *     () -> dbOperation(),
     *     SQLException::isTransient,
     *     () -> {},
     *     3, 100L,
     *     listener
     * );
     * }</pre>
     *
     * @return logging listener
     */
    static RetryListener logging() {
      return (attempt, exception) ->
          LOGGER.log(
              WARNING, "Retry attempt {0} after exception: {1}", attempt, exception.getMessage());
    }
  }

  /**
   * Pluggable authentication error detector for identifying auth failures in SQLExceptions.
   *
   * <p>Allows customization of authentication error detection logic to support vendor-specific
   * error codes and messages.
   *
   * <h3>Using Default Detector</h3>
   *
   * <pre>{@code
   * final var detector = AuthErrorDetector.defaultDetector();
   * if (detector.isAuthError(sqlException)) {
   *     System.out.println("Authentication error detected");
   * }
   * }</pre>
   *
   * <h3>Custom Detector for Oracle</h3>
   *
   * <pre>{@code
   * final var oracleDetector = AuthErrorDetector.custom(e ->
   *     e.getErrorCode() == 1017 ||  // ORA-01017: invalid username/password
   *     e.getErrorCode() == 28000 ||  // ORA-28000: account is locked
   *     e.getErrorCode() == 1005      // ORA-01005: null password
   * );
   *
   * final var rotatingDs = new RotatingDataSource(
   *     secretId,
   *     factory,
   *     0L,
   *     Retry.Policy.fixed(2, 50L),
   *     oracleDetector
   * );
   * }</pre>
   *
   * <h3>Combining Detectors</h3>
   *
   * <pre>{@code
   * final var mysqlDetector = AuthErrorDetector.custom(e ->
   *     e.getErrorCode() == 1045  // Access denied
   * );
   *
   * var combined = AuthErrorDetector.defaultDetector()
   *     .or(mysqlDetector);
   *
   * Retry.authRetry(
   *     () -> repository.findAll(),
   *     rotatingDataSource,
   *     combined
   * );
   * }</pre>
   */
  @FunctionalInterface
  public interface AuthErrorDetector {
    /**
     * Determines if the exception represents an authentication or authorization error.
     *
     * @param e the exception to check
     * @return true if it's an authentication error
     */
    boolean isAuthError(final SQLException e);

    /**
     * Returns the default detector using built-in heuristics.
     *
     * <p>Checks for:
     *
     * <ul>
     *   <li>SQLState codes: 28000 (invalid authorization), 28P01 (invalid password - PostgreSQL)
     *   <li>Message keywords: "access denied", "authentication failed", "password authentication
     *       failed", "invalid password", "permission denied"
     * </ul>
     *
     * <p>Example:
     *
     * <pre>{@code
     * final var detector = AuthErrorDetector.defaultDetector();
     *
     * try {
     *     connection.createStatement().execute("SELECT 1");
     * } catch (SQLException e) {
     *     if (detector.isAuthError(e)) {
     *         System.out.println("Auth error: resetting credentials");
     *         rotatingDs.reset();
     *     }
     * }
     * }</pre>
     *
     * @return default detector
     */
    static AuthErrorDetector defaultDetector() {
      return Retry::isAuthError;
    }

    /**
     * Creates a custom detector from a predicate.
     *
     * <p>Allows implementing vendor-specific auth error detection logic.
     *
     * <p>Example for PostgreSQL:
     *
     * <pre>{@code
     * var pgDetector = AuthErrorDetector.custom(e -> {
     *     final var state = e.getSQLState();
     *     return "28000".equals(state) ||  // invalid_authorization_specification
     *            "28P01".equals(state) ||  // invalid_password
     *            "3D000".equals(state);    // invalid_catalog_name (database doesn't exist)
     * });
     * }</pre>
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
     * <p>Useful for supporting multiple database vendors or adding custom detection logic on top of
     * the default.
     *
     * <p>Example:
     *
     * <pre>{@code
     * // Support both default heuristics and Oracle-specific error codes
     * var combined = AuthErrorDetector.defaultDetector()
     *     .or(AuthErrorDetector.custom(e -> e.getErrorCode() == 1017));
     *
     * final var result = Retry.authRetry(
     *     () -> executeQuery(),
     *     rotatingDataSource,
     *     combined
     * );
     * }</pre>
     *
     * @param other the other detector to combine with
     * @return combined detector that returns true if either detector matches
     */
    default AuthErrorDetector or(final AuthErrorDetector other) {
      return e -> this.isAuthError(e) || other.isAuthError(e);
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
   * Executes the supplier with the given retry policy.
   *
   * <p>Supports both fixed delay and exponential backoff strategies based on the policy
   * configuration.
   *
   * <h3>Fixed Delay Example</h3>
   *
   * <pre>{@code
   * final var policy = Policy.fixed(3, 200L);
   *
   * List<User> users = Retry.onException(
   *     () -> userRepository.findAll(),
   *     SQLException::isTransient,
   *     () -> {},
   *     policy
   * );
   * }</pre>
   *
   * <h3>Exponential Backoff Example</h3>
   *
   * <pre>{@code
   * final var policy = Policy.exponential(5, 100L);
   *
   * final var result = Retry.onException(
   *     () -> {
   *         try (var stmt = connection.createStatement()) {
   *             var rs = stmt.executeQuery("SELECT data FROM table");
   *             return rs.next() ? rs.getString(1) : null;
   *         }
   *     },
   *     e -> e.getSQLState().startsWith("40"), // serialization failures
   *     () -> System.out.println("Transaction conflict, retrying with backoff..."),
   *     policy
   * );
   * }</pre>
   *
   * @param supplier operation to execute
   * @param shouldRetry predicate determining whether the exception is retryable
   * @param beforeRetry hook to run before each retry attempt
   * @param policy retry policy configuration
   * @param <T> result type
   * @param <E> exception type extending SQLException
   * @return the supplier result
   * @throws E if all attempts fail or the exception is not retryable
   */
  public static <T, E extends SQLException> T onException(
      final SqlExceptionSupplier<T, E> supplier,
      final Predicate<E> shouldRetry,
      final Runnable beforeRetry,
      final Policy policy)
      throws E {
    var attempt = 0;
    while (true) {
      attempt++;
      try {
        return supplier.get();
      } catch (final SQLException ex) {
        @SuppressWarnings("unchecked")
        final E typedException = (E) ex;

        if (!shouldRetry.test(typedException) || attempt >= policy.maxAttempts()) {
          throw typedException;
        }

        LOGGER.log(DEBUG, "Attempt {0} failed, retrying with policy...", attempt);
        beforeRetry.run();

        final var delay = policy.calculateDelay(attempt);
        if (delay > 0) {
          try {
            Thread.sleep(delay);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw typedException;
          }
        }
      }
    }
  }

  /**
   * Executes the supplier with retry listener for observability.
   *
   * <p>The listener is notified before each retry attempt (but not before the first attempt),
   * useful for metrics, monitoring, and debugging.
   *
   * <h3>Logging Example</h3>
   *
   * <pre>{@code
   * final var listener = RetryListener.logging();
   *
   * String result = Retry.onException(
   *     () -> executeQuery(),
   *     SQLException::isTransient,
   *     () -> resetConnection(),
   *     3, 200L,
   *     listener
   * );
   * }</pre>
   *
   * <h3>Metrics Example</h3>
   *
   * <pre>{@code
   * final var listener = (attempt, exception) -> {
   *     System.out.printf("Retry %d due to: %s%n", attempt, exception.getMessage());
   *     metrics.increment("db.retries",
   *         Tags.of("attempt", String.valueOf(attempt),
   *                 "error", exception.getClass().getSimpleName())
   *     );
   * };
   *
   * final var orders = Retry.onException(
   *     () -> orderRepository.findPending(),
   *     e -> e.getSQLState().startsWith("08"), // connection errors
   *     () -> {},
   *     5, 500L,
   *     listener
   * );
   * }</pre>
   *
   * @param supplier operation to execute
   * @param shouldRetry predicate determining whether the exception is retryable
   * @param beforeRetry hook to run before each retry attempt
   * @param maxAttempts total attempts including the first (must be >= 1)
   * @param delayMillis delay between attempts in milliseconds (non-negative)
   * @param listener listener to be notified of retry events
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
      final long delayMillis,
      final RetryListener listener)
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

        listener.onRetry(attempt, typedException);

        if (!shouldRetry.test(typedException) || attempt >= maxAttempts) {
          throw typedException;
        }

        LOGGER.log(DEBUG, "Attempt {0} failed, retrying with listener...", attempt);
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
   * Convenience method for ORM layers: runs an operation with automatic auth error retry.
   *
   * <p>If the operation fails with a RuntimeException containing an authentication-related
   * SQLException, the rotating data source is reset and the operation is retried once.
   *
   * <h3>Hibernate Example</h3>
   *
   * <pre>{@code
   * var users = Retry.authRetry(() -> {
   *     try (var session = sessionFactory.openSession()) {
   *         return session.createQuery("FROM User", User.class).list();
   *     }
   * }, rotatingDataSource);
   * }</pre>
   *
   * <h3>JPA Example</h3>
   *
   * <pre>{@code
   * final var orders = Retry.authRetry(
   *     () -> entityManager
   *         .createQuery("SELECT o FROM Order o WHERE o.status = :status", Order.class)
   *         .setParameter("status", "PENDING")
   *         .getResultList(),
   *     rotatingDataSource
   * );
   * }</pre>
   *
   * <h3>Spring Data Example</h3>
   *
   * <pre>{@code
   * Optional<User> user = Retry.authRetry(
   *     () -> userRepository.findByEmail("user@example.com"),
   *     rotatingDataSource
   * );
   * }</pre>
   *
   * @param <T> result type
   * @param op operation to run
   * @param rotating data source wrapper
   * @return the operation result
   * @throws RuntimeException if the operation fails after retry
   */
  public static <T> T authRetry(final Supplier<? extends T> op, final RotatingDataSource rotating) {
    return authRetry(op, rotating::reset);
  }

  /**
   * Auth retry with custom authentication error detector.
   *
   * <p>Allows using vendor-specific error detection logic instead of the default heuristics.
   *
   * <h3>Oracle Example</h3>
   *
   * <pre>{@code
   * final var oracleDetector = AuthErrorDetector.custom(e ->
   *     e.getErrorCode() == 1017 ||  // invalid username/password
   *     e.getErrorCode() == 28000    // account is locked
   * );
   *
   * var result = Retry.authRetry(
   *     () -> repository.findAll(),
   *     rotatingDataSource,
   *     oracleDetector
   * );
   * }</pre>
   *
   * <h3>MySQL Example</h3>
   *
   * <pre>{@code
   * var mysqlDetector = AuthErrorDetector.custom(e ->
   *     e.getErrorCode() == 1045 ||  // Access denied
   *     e.getErrorCode() == 1044     // Access denied for database
   * );
   *
   * final var products = Retry.authRetry(
   *     () -> productRepository.findAll(),
   *     rotatingDataSource,
   *     mysqlDetector
   * );
   * }</pre>
   *
   * @param op operation to run
   * @param rotating rotating data source
   * @param detector custom authentication error detector
   * @param <T> result type
   * @return the operation result
   * @throws RuntimeException if the operation fails after retry
   */
  public static <T> T authRetry(
      final Supplier<? extends T> op,
      final RotatingDataSource rotating,
      final AuthErrorDetector detector) {
    return authRetry(op, rotating::reset, detector);
  }

  /**
   * Auth retry with explicit result type to avoid unchecked casts.
   *
   * <p>Useful when calling JPA/ORM APIs that return {@code Object} but you know the actual type.
   *
   * <h3>Native Query Example</h3>
   *
   * <pre>{@code
   * final var timestamp = Retry.authRetry(
   *     () -> entityManager
   *         .createNativeQuery("SELECT now()")
   *         .getSingleResult(),
   *     rotatingDataSource,
   *     Instant.class
   * );
   * }</pre>
   *
   * <h3>Stored Procedure Example</h3>
   *
   * <pre>{@code
   * final var userId = Retry.authRetry(
   *     () -> entityManager
   *         .createStoredProcedureQuery("create_user")
   *         .registerStoredProcedureParameter(1, String.class, ParameterMode.IN)
   *         .registerStoredProcedureParameter(2, Integer.class, ParameterMode.OUT)
   *         .setParameter(1, "john@example.com")
   *         .execute()
   *         .getOutputParameterValue(2),
   *     rotatingDataSource,
   *     Integer.class
   * );
   * }</pre>
   *
   * @param op the operation to run
   * @param rotating rotating data source wrapper
   * @param expectedType the expected result type token
   * @param <T> expected result type
   * @return the result cast to the expected type
   * @throws RuntimeException if the operation fails after retry
   * @throws ClassCastException if the result cannot be cast to the expected type
   */
  public static <T> T authRetry(
      final Supplier<?> op, final RotatingDataSource rotating, final Class<T> expectedType) {
    return expectedType.cast(authRetry(op, rotating::reset));
  }

  /**
   * Void-returning auth retry for operations with side effects.
   *
   * <p>Useful for update, insert, delete operations that don't return values.
   *
   * <h3>Hibernate Example</h3>
   *
   * <pre>{@code
   * Retry.authRetry(() -> {
   *     try (var session = sessionFactory.openSession()) {
   *         var tx = session.beginTransaction();
   *         session.persist(newUser);
   *         tx.commit();
   *     }
   * }, rotatingDataSource);
   * }</pre>
   *
   * <h3>JPA Example</h3>
   *
   * <pre>{@code
   * Retry.authRetry(() -> {
   *     entityManager.getTransaction().begin();
   *     entityManager.persist(newProduct);
   *     entityManager.flush();
   *     entityManager.getTransaction().commit();
   * }, rotatingDataSource);
   * }</pre>
   *
   * <h3>Spring @Transactional Example</h3>
   *
   * <pre>{@code
   * @Transactional
   * public void updateUser(Long id, String newEmail) {
   *     Retry.authRetry(() -> {
   *         User user = userRepository.findById(id).orElseThrow();
   *         user.setEmail(newEmail);
   *         userRepository.save(user);
   *     }, rotatingDataSource);
   * }
   * }</pre>
   *
   * @param op operation to run
   * @param rotating data source wrapper
   */
  public static void authRetry(final Runnable op, final RotatingDataSource rotating) {
    authRetry(
        (Supplier<Void>)
            () -> {
              op.run();
              return null;
            },
        rotating);
  }

  /**
   * Retries the given supplier on transient connection/pool errors using a default short policy.
   *
   * <p>Default policy: up to 3 attempts with 50ms delay between attempts.
   *
   * <p>Designed for ORM operations that may encounter transient connection issues during pool swaps
   * or network hiccups.
   *
   * <h3>Hibernate Example</h3>
   *
   * <pre>{@code
   * final var users = Retry.transientRetry(() -> {
   *     try (var session = sessionFactory.openSession()) {
   *         return session.createQuery("FROM User", User.class).list();
   *     }
   * });
   * }</pre>
   *
   * <h3>JPA Example</h3>
   *
   * <pre>{@code
   * final var product = Retry.transientRetry(() ->
   *     entityManager.find(Product.class, productId)
   * );
   * }</pre>
   *
   * @param <T> result type
   * @param op operation to execute
   * @return operation result
   * @throws RuntimeException if all attempts fail
   */
  public static <T> T transientRetry(final Supplier<? extends T> op) {
    return transientRetry(op, Policy.fixed(3, 50L));
  }

  /**
   * Retries the given supplier on transient connection/pool errors using the provided policy.
   *
   * <p>Wraps any SQLException with transient connection error characteristics in retry logic.
   * Useful for handling temporary pool exhaustion, connection resets, or network hiccups.
   *
   * <h3>Fixed Delay Example</h3>
   *
   * <pre>{@code
   * final var policy = Policy.fixed(5, 100L);
   *
   * final var orders = Retry.transientRetry(() -> {
   *     try (var session = sessionFactory.openSession()) {
   *         return session
   *             .createQuery("FROM Order WHERE status = :status", Order.class)
   *             .setParameter("status", "PENDING")
   *             .list();
   *     }
   * }, policy);
   * }</pre>
   *
   * <h3>Exponential Backoff Example</h3>
   *
   * <pre>{@code
   * final var policy = Policy.exponential(7, 100L);
   *
   * final var result = Retry.transientRetry(() -> {
   *     try (var session = sessionFactory.openSession()) {
   *         var tx = session.beginTransaction();
   *         session.persist(newEntity);
   *         tx.commit();
   *         return newEntity.getId();
   *     }
   * }, policy);
   * }</pre>
   *
   * @param <T> result type
   * @param op operation to execute
   * @param policy retry policy configuration
   * @return operation result
   * @throws RuntimeException if all attempts fail or non-transient error occurs
   */
  static <T> T transientRetry(final Supplier<? extends T> op, final Policy policy) {
    int attempt = 0;
    while (true) {
      try {
        return op.get();
      } catch (final RuntimeException re) {
        final var sql = findSqlException(re);
        if (isTransientConnectionError(sql)) {
          final int nextAttempt = attempt + 1;
          if (nextAttempt > policy.maxAttempts()) {
            LOGGER.log(WARNING, "Transient error retry exhausted after {0} attempts", nextAttempt);
            throw re;
          }
          final long delay = Math.min(policy.maxDelayMillis(), policy.calculateDelay(nextAttempt));
          LOGGER.log(
              DEBUG,
              "Transient connection error on attempt {0}, retrying in {1}ms: {2}",
              nextAttempt,
              delay,
              sql.getMessage());
          try {
            Thread.sleep(delay);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw re;
          }
          attempt = nextAttempt;
          continue;
        }
        throw re;
      }
    }
  }

  /**
   * Void-returning overload for transient retries.
   *
   * <p>Useful for write operations that don't return a value.
   *
   * <h3>Insert Example</h3>
   *
   * <pre>{@code
   * Retry.transientRetry(() -> {
   *     try (var session = sessionFactory.openSession()) {
   *         var tx = session.beginTransaction();
   *         session.persist(newUser);
   *         tx.commit();
   *     }
   * });
   * }</pre>
   *
   * <h3>Update Example</h3>
   *
   * <pre>{@code
   * Retry.transientRetry(() -> {
   *     try (var session = sessionFactory.openSession()) {
   *         var tx = session.beginTransaction();
   *         var user = session.get(User.class, userId);
   *         user.setEmail(newEmail);
   *         tx.commit();
   *     }
   * });
   * }</pre>
   *
   * @param op operation to execute
   * @throws RuntimeException if all attempts fail
   */
  public static void transientRetry(final Runnable op) {
    transientRetry(
        (Supplier<Void>)
            () -> {
              op.run();
              return null;
            });
  }

  // Package-private to keep API minimal while enabling testing and potential reuse.
  static <T> T authRetry(final Supplier<? extends T> op, final Runnable reset) {
    return authRetry(op, reset, AuthErrorDetector.defaultDetector());
  }

  static <T> T authRetry(
      final Supplier<? extends T> op, final Runnable reset, final AuthErrorDetector detector) {
    try {
      return op.get();
    } catch (final RuntimeException e) {
      final var sql = findSqlException(e);
      if (sql != null && detector.isAuthError(sql)) {
        reset.run();
        return op.get();
      }
      throw e;
    }
  }

  /**
   * Finds the first SQLException in a throwable cause chain.
   *
   * <p>Useful for unwrapping exceptions thrown by ORM frameworks which often wrap SQLExceptions in
   * RuntimeExceptions.
   *
   * <h3>Basic Example</h3>
   *
   * <pre>{@code
   * try {
   *     hibernateTemplate.execute(session -> {
   *         return session.createQuery("FROM User").list();
   *     });
   * } catch (RuntimeException e) {
   *     final var sql = Retry.findSqlException(e);
   *     if (sql != null) {
   *         System.out.println("SQL State: " + sql.getSQLState());
   *         System.out.println("Error Code: " + sql.getErrorCode());
   *     }
   * }
   * }</pre>
   *
   * <h3>Auth Error Detection Example</h3>
   *
   * <pre>{@code
   * try {
   *     entityManager.persist(newUser);
   * } catch (RuntimeException e) {
   *     final var sql = Retry.findSqlException(e);
   *     if (sql != null && Retry.isAuthError(sql)) {
   *         System.out.println("Authentication failure detected");
   *         rotatingDataSource.reset();
   *     }
   * }
   * }</pre>
   *
   * @param t the throwable to search
   * @return the first SQLException in the chain, or null if none found
   */
  public static SQLException findSqlException(final Throwable t) {
    Throwable cur = t;
    SQLException last = null;
    while (cur != null) {
      if (cur instanceof SQLException se) last = se; // keep deepest SQLException
      cur = cur.getCause();
    }
    return last;
  }

  /**
   * Heuristic to detect authentication errors based on SQLState and common message patterns.
   *
   * <p>Checks for:
   *
   * <ul>
   *   <li>SQLState codes:
   *       <ul>
   *         <li>{@code 28000} - invalid authorization specification
   *         <li>{@code 28P01} - invalid password (PostgreSQL-specific)
   *       </ul>
   *   <li>Message keywords (case-insensitive):
   *       <ul>
   *         <li>"access denied"
   *         <li>"authentication failed"
   *         <li>"password authentication failed"
   *         <li>"invalid password"
   *         <li>"permission denied"
   *       </ul>
   * </ul>
   *
   * <h3>Basic Usage</h3>
   *
   * <pre>{@code
   * try {
   *     connection.createStatement().execute("SELECT 1");
   * } catch (SQLException e) {
   *     if (Retry.isAuthError(e)) {
   *         System.out.println("Authentication failure detected");
   *         rotatingDataSource.reset();
   *     }
   * }
   * }</pre>
   *
   * <h3>Custom Retry Logic</h3>
   *
   * <pre>{@code
   * final var result = Retry.onException(
   *     () -> executeQuery(),
   *     Retry::isAuthError,
   *     () -> {
   *         System.out.println("Auth error, rotating credentials...");
   *         rotatingDataSource.reset();
   *     },
   *     2, 0L
   * );
   * }</pre>
   *
   * @param e the exception to check
   * @return true if the exception is an authentication error
   */
  public static boolean isAuthError(final SQLException e) {
    if (e == null) return false;

    final var state = e.getSQLState();

    // Auth-specific SQLStates only
    final var isAuthState = "28000".equals(state) || "28P01".equals(state);

    final var msg = e.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (final var kw : AUTH_KEYWORDS) if (lower.contains(kw)) return true;
    }

    return isAuthState;
  }

  /**
   * Detects transient connection/pool errors likely to recover on retry.
   *
   * <p>Heuristics used:
   *
   * <ul>
   *   <li>SQLState class 08 (connection exception)
   *   <li>Instance of {@code SQLTransientException} (includes {@code
   *       SQLTransientConnectionException})
   *   <li>Known transient keywords in message: "connection refused", "connection reset", "i/o
   *       error", "socket closed", "broken pipe", "pool", "closed", "timeout"
   * </ul>
   *
   * <h3>Basic Usage</h3>
   *
   * <pre>{@code
   * try {
   *     connection.createStatement().execute("SELECT 1");
   * } catch (SQLException e) {
   *     if (Retry.isTransientConnectionError(e)) {
   *         System.out.println("Transient connection error, retrying...");
   *         // retry logic
   *     }
   * }
   * }</pre>
   *
   * <h3>Custom Retry Handler</h3>
   *
   * <pre>{@code
   * final var result = Retry.onException(
   *     () -> executeQuery(),
   *     Retry::isTransientConnectionError,
   *     () -> System.out.println("Connection issue, retrying..."),
   *     5, 200L
   * );
   * }</pre>
   *
   * @param e the exception to check
   * @return true if the exception is a transient connection error
   */
  static boolean isTransientConnectionError(final SQLException e) {
    if (e == null) return false;

    final var state = e.getSQLState();
    if (state != null && state.startsWith("08")) return true;

    // Transient class hierarchy
    if (e instanceof SQLTransientException) return true;

    if (e.getClass().getSimpleName().contains("JDBCConnectionException")) return true;

    final var msg = e.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (final var kw : TRANSIENT_CONN_KEYWORDS) if (lower.contains(kw)) return true;
    }

    return false;
  }
}
