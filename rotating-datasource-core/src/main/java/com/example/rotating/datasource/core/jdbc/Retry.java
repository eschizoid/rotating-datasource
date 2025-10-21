package com.example.rotating.datasource.core.jdbc;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Retry utilities used by {@link RotatingDataSource} and optionally available for advanced use in
 * applications.
 *
 * <p>Provides two categories of behavior:
 *
 * <ul>
 *   <li><b>Automatic</b> — When you use RotatingDataSource, authentication and transient connection
 *       failures during {@code getConnection()} are handled automatically. ORMs using the
 *       DataSource inherit this behavior. No direct use of this class is required for the common
 *       case.
 *   <li><b>Opt-in (advanced)</b> — For JDBC code paths where you want additional retries around a
 *       particular operation, use {@link #onException(SqlExceptionSupplier,
 *       java.util.function.Predicate, Runnable, Policy)}. For ORM unit-of-work retries across a
 *       password cutover, you may wrap the operation with {@link
 *       #authRetry(java.util.function.Supplier, RotatingDataSource, AuthErrorDetector)}. Most
 *       applications won't need this.
 * </ul>
 *
 * <h2>Typical usage</h2>
 *
 * <p>JDBC (optional, advanced):
 *
 * <pre>{@code
 * final var policy = Retry.Policy.exponential(5, 100L);
 * final var list = Retry.onException(
 *     () -> jdbcTemplate.query("SELECT * FROM t", mapper),
 *     SQLException::isTransient,
 *     () -> {},
 *     policy
 * );
 * }</pre>
 *
 * <p>ORMs (rarely needed):
 *
 * <pre>{@code
 * final var users = Retry.authRetry(
 *     () -> entityManager.createQuery("FROM User", User.class).getResultList(),
 *     rotatingDataSource,
 *     Retry.AuthErrorDetector.defaultDetector()
 * );
 * }</pre>
 *
 * @see RotatingDataSource
 * @see Policy
 * @see AuthErrorDetector
 * @see RetryListener
 */
public final class Retry {

  private static final System.Logger LOGGER = System.getLogger(Retry.class.getName());
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

  private Retry() {}

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
   * @param policy retry policy configuration
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
      final Policy policy,
      final RetryListener listener)
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

        listener.onRetry(attempt, typedException);

        LOGGER.log(DEBUG, "Attempt {0} failed, retrying with listener...", attempt);
        beforeRetry.run();

        final long delay = policy.calculateDelay(attempt);
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
  static <T> T authRetry(
      final Supplier<? extends T> op,
      final RotatingDataSource rotating,
      final AuthErrorDetector detector) {
    return authRetry(op, rotating::reset, detector);
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
          if (attempt >= policy.maxAttempts()) {
            LOGGER.log(WARNING, "Transient error retry exhausted after {0} attempts", attempt);
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

  static <T> T authRetry(final Supplier<? extends T> op, final Runnable reset) {
    return authRetry(op, reset, AuthErrorDetector.defaultDetector());
  }

  /**
   * Executes an operation with authentication error retry using custom detector.
   *
   * <p>If the operation throws a RuntimeException wrapping an authentication-related SQLException,
   * the reset hook is executed and the operation is retried once.
   *
   * <h3>Custom Reset Hook Example</h3>
   *
   * <pre>{@code
   * var result = Retry.authRetry(
   *     () -> executeQuery(),
   *     () -> connectionPool.evictAll(),  // Custom reset
   *     AuthErrorDetector.custom(e -> e.getErrorCode() == 1017)
   * );
   * }</pre>
   *
   * <h3>Multiple Data Sources Example</h3>
   *
   * <pre>{@code
   * var readResult = Retry.authRetry(
   *     () -> readOnlyRepository.findAll(),
   *     readOnlyDs::reset,
   *     AuthErrorDetector.defaultDetector()
   * );
   *
   * var writeResult = Retry.authRetry(
   *     () -> primaryRepository.save(entity),
   *     primaryDs::reset,
   *     AuthErrorDetector.defaultDetector()
   * );
   * }</pre>
   *
   * @param op operation to execute
   * @param reset callback to execute before retry (e.g., credential refresh)
   * @param detector predicate for identifying authentication errors
   * @param <T> result type
   * @return operation result
   * @throws RuntimeException if operation fails after retry or on non-auth errors
   */
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
  static SQLException findSqlException(final Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof SQLException sql) return sql;
      cur = cur.getCause();
    }
    return null;
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
   * Detects transient connection errors that warrant automatic retry.
   *
   * <p>Identifies temporary failures that typically resolve quickly:
   *
   * <ul>
   *   <li>Connection timeouts (SQLState 08xxx)
   *   <li>Network interruptions (connection refused/reset)
   *   <li>Pool exhaustion
   *   <li>SQLTransientException subclasses
   * </ul>
   *
   * <h3>Usage in Custom Retry Logic</h3>
   *
   * <pre>{@code
   * try {
   *     return executeQuery();
   * } catch (SQLException e) {
   *     if (Retry.isTransientConnectionError(e)) {
   *         // Retry automatically
   *         return executeQuery();
   *     }
   *     throw e;  // Non-retryable error
   * }
   * }</pre>
   *
   * @param e the SQLException to check (null-safe)
   * @return true if the error is transient and retryable
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
      for (final var kw : TRANSIENT_CONN_KEYWORDS) if (lower.contains(kw)) return true;
    }

    return false;
  }

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

    /**
     * Called before each retry attempt (but not before the first attempt).
     *
     * @param attempt attempt number (1-based) that just failed
     * @param exception the exception that triggered the retry
     */
    void onRetry(final int attempt, final Throwable exception);
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
     * Determines if the exception represents an authentication or authorization error.
     *
     * @param e the exception to check
     * @return true if it's an authentication error
     */
    boolean isAuthError(final SQLException e);

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
   * Retry policy defining attempt limits and backoff strategy.
   *
   * <h3>Jitter Mechanics</h3>
   *
   * <p>When enabled, jitter adds randomness to prevent thundering herd:
   *
   * <ul>
   *   <li>Random multiplier between 0.75 and 1.25 (±25% variance)
   *   <li>Example: 1000ms delay becomes 750ms-1250ms
   *   <li>Spreads retry timing across multiple clients
   * </ul>
   *
   * <h3>Examples</h3>
   *
   * <pre>{@code
   * // Fixed: exactly 500ms between attempts
   * var fixed = Policy.fixed(3, 500L);
   *
   * // Exponential: 100ms, ~200ms, ~400ms with jitter
   * var exponential = Policy.exponential(5, 100L);
   *
   * // Custom: capped at 10s with 3x multiplier
   * var custom = new Policy(10, 100L, 10_000L, 3.0, true);
   * }</pre>
   *
   * @param maxAttempts total attempts (first + retries), must be ≥ 1
   * @param initialDelayMillis starting delay, must be ≥ 0
   * @param maxDelayMillis cap for exponential growth, must be ≥ initialDelayMillis
   * @param backoffMultiplier growth factor (1.0 = fixed), must be ≥ 1.0
   * @param jitter whether to add ±25% randomness to delays
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
     * <p>Delays grow exponentially (2x) up to a maximum of 60 seconds, with 25% random jitter added
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
      return new Policy(attempts, initialDelay, 60_000L, 2.0, true);
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
}
