package com.example.rotatingdatasource.core.reactive;

import com.example.rotatingdatasource.core.jdbc.Retry;
import io.r2dbc.spi.R2dbcException;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * Reactive retry utilities and detectors for R2DBC, mirroring the capabilities and style of {@link
 * Retry} for JDBC.
 *
 * <h2>Authentication Error Detection</h2>
 *
 * <pre>{@code
 * // Use default detector
 * var detector = R2dbcRetry.AuthErrorDetector.defaultDetector();
 *
 * // Combine with a custom vendor-specific detector
 * var custom = R2dbcRetry.AuthErrorDetector.custom(t ->
 *     (t instanceof io.r2dbc.spi.R2dbcException ex) &&
 *     "28P01".equals(ex.getSqlState()) // invalid password (PostgreSQL)
 * );
 *
 * var combined = detector.or(custom);
 * }</pre>
 *
 * <h2>Transient Connection Error Heuristics</h2>
 *
 * <pre>{@code
 * reactor.util.retry.Retry.backoff(10, Duration.ofSeconds(1))
 *     .filter(R2dbcRetry::isTransientConnectionError)
 *     .doBeforeRetry(sig -> LOGGER.log(System.Logger.Level.DEBUG,
 *         "Transient error on attempt {0}", sig.totalRetries() + 1));
 * }</pre>
 */
public final class R2dbcRetry {
  private R2dbcRetry() {}

  private static final String[] AUTH_KEYWORDS = {
    "access denied",
    "authentication failed",
    "password authentication failed",
    "invalid password",
    "permission denied"
  };

  private static final String[] TRANSIENT_KEYWORDS = {
    "connection refused",
    "connection reset",
    "i/o error",
    "socket closed",
    "broken pipe",
    "timeout",
    "pool"
  };

  /**
   * Determines if a Throwable represents an authentication or authorization error using built-in
   * heuristics.
   *
   * <ul>
   *   <li>SQLSTATE: 28000 (invalid authorization), 28P01 (invalid password - PostgreSQL)
   *   <li>Message keywords: access denied, authentication failed, invalid password, permission
   *       denied
   * </ul>
   */
  public static boolean isAuthError(final Throwable error) {
    if (!(error instanceof R2dbcException r2dbcEx)) return false;

    final var sqlState = r2dbcEx.getSqlState();
    if ("28000".equals(sqlState) || "28P01".equals(sqlState)) return true;

    final var msg = r2dbcEx.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (var keyword : AUTH_KEYWORDS) if (lower.contains(keyword)) return true;
    }

    return false;
  }

  /**
   * Heuristic detector for transient connectivity issues suitable for reactive retry policies.
   *
   * <ul>
   *   <li>SQLSTATE classes starting with 08 (connection exceptions)
   *   <li>Common transport-level keywords such as connection refused/reset, timeout, etc.
   * </ul>
   */
  public static boolean isTransientConnectionError(final Throwable error) {
    if (!(error instanceof R2dbcException r2dbcEx)) return false;

    final var sqlState = r2dbcEx.getSqlState();
    if (sqlState != null && sqlState.startsWith("08")) return true;

    final var msg = r2dbcEx.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (var keyword : TRANSIENT_KEYWORDS) if (lower.contains(keyword)) return true;
    }

    return false;
  }

  /**
   * Pluggable authentication error detector for identifying auth failures for R2DBC drivers.
   *
   * <p>Allows customization of authentication error detection logic to support vendor-specific
   * error codes and messages.
   *
   * <h3>Using Default Detector</h3>
   *
   * <pre>{@code
   * final var detector = R2dbcRetry.AuthErrorDetector.defaultDetector();
   * if (detector.isAuthError(throwable)) {
   *     System.out.println("Authentication error detected");
   * }
   * }</pre>
   *
   * <h3>Combining Detectors</h3>
   *
   * <pre>{@code
   * var combined = R2dbcRetry.AuthErrorDetector.defaultDetector()
   *     .or(R2dbcRetry.AuthErrorDetector.custom(t -> true));
   * }</pre>
   */
  @FunctionalInterface
  public interface AuthErrorDetector {
    /** Returns true if the provided Throwable represents an authentication error. */
    boolean isAuthError(Throwable error);

    /**
     * Returns the default detector which delegates to {@link R2dbcRetry#isAuthError(Throwable)}.
     */
    static AuthErrorDetector defaultDetector() {
      return R2dbcRetry::isAuthError;
    }

    /** Creates a custom detector from a predicate. */
    static AuthErrorDetector custom(final Predicate<Throwable> predicate) {
      return predicate::test;
    }

    /** Combines this detector with another using OR semantics. */
    default AuthErrorDetector or(final AuthErrorDetector other) {
      return e -> this.isAuthError(e) || other.isAuthError(e);
    }
  }
}
