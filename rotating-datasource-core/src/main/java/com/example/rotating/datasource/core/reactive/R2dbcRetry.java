package com.example.rotating.datasource.core.reactive;

import com.example.rotating.datasource.core.jdbc.Retry;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.time.Duration;
import java.util.Locale;
import java.util.function.Predicate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

  private R2dbcRetry() {}

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
    final var r2dbcEx = findR2dbcException(error);
    if (r2dbcEx != null) {
      final var sqlState = r2dbcEx.getSqlState();
      if ("28000".equals(sqlState) || "28P01".equals(sqlState)) return true;

      final var msg = r2dbcEx.getMessage();
      if (msg != null) {
        final var lower = msg.toLowerCase(Locale.ROOT);
        for (var keyword : AUTH_KEYWORDS) if (lower.contains(keyword)) return true;
      }
    }

    // Fallback to top-level message keywords (e.g., pool wrappers)
    final var topMsg = error != null ? error.getMessage() : null;
    if (topMsg != null) {
      final var lower = topMsg.toLowerCase(Locale.ROOT);
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
    // Unwrap to first R2dbcException if present
    final var r2dbcEx = findR2dbcException(error);
    if (r2dbcEx != null) {
      // Known transient type
      if (r2dbcEx instanceof R2dbcTransientResourceException) return true;

      final var sqlState = r2dbcEx.getSqlState();
      if (sqlState != null && sqlState.startsWith("08")) return true;

      final var msg = r2dbcEx.getMessage();
      if (msg != null) {
        final var lower = msg.toLowerCase(Locale.ROOT);
        for (var keyword : TRANSIENT_KEYWORDS) if (lower.contains(keyword)) return true;
      }
    }

    // Fallback to top-level message heuristics (e.g., pool exhaustion wrappers)
    final var topMsg = error != null ? error.getMessage() : null;
    if (topMsg != null) {
      final var lower = topMsg.toLowerCase(Locale.ROOT);
      for (var keyword : TRANSIENT_KEYWORDS) if (lower.contains(keyword)) return true;
    }

    return false;
  }

  /**
   * Returns a predicate that matches transient connection/pool errors suitable for Reactor Retry
   * filtering.
   */
  public static Predicate<Throwable> transientErrorPredicate() {
    return R2dbcRetry::isTransientConnectionError;
  }

  /**
   * Creates a fixed-delay Reactor retry spec analogous to {@link Retry.Policy#fixed(int, long)}.
   * The number of resubscriptions equals (attempts - 1).
   */
  public static reactor.util.retry.Retry fixed(final int attempts, final Duration delay) {
    final int retries = Math.max(0, attempts - 1);
    return reactor.util.retry.Retry.fixedDelay(retries, delay);
  }

  /**
   * Creates an exponential backoff Reactor retry spec with optional max backoff and jitter
   * analogous to {@link Retry.Policy#exponential(int, long)} and custom policies.
   */
  public static reactor.util.retry.Retry exponential(
      final int attempts, final Duration initial, final Duration max) {
    final int retries = Math.max(0, attempts - 1);
    return reactor.util.retry.Retry.backoff(retries, initial).maxBackoff(max).jitter(0.25d);
  }

  /** Adapts a JDBC {@link Retry.Policy} to a Reactor retry spec. */
  public static reactor.util.retry.Retry toReactorRetry(final Retry.Policy p) {
    final int retries = Math.max(0, p.maxAttempts() - 1);
    reactor.util.retry.Retry spec;
    if (p.backoffMultiplier() > 1.0)
      spec =
          reactor.util.retry.Retry.backoff(retries, Duration.ofMillis(p.initialDelayMillis()))
              .maxBackoff(Duration.ofMillis(p.maxDelayMillis()));
    else
      spec =
          reactor.util.retry.Retry.fixedDelay(retries, Duration.ofMillis(p.initialDelayMillis()));

    // Note: the current Reactor Retry API in this project does not expose jitter configuration.
    return spec;
  }

  /**
   * Convenience wrapper: retry a Mono once on authentication error by invoking the provided reset
   * action, then resubscribing.
   */
  public static <T> Mono<T> authRetry(
      final Mono<T> source, final Mono<Void> reset, final AuthErrorDetector detector) {
    return source.onErrorResume(t -> detector.isAuthError(t) ? reset.then(source) : Mono.error(t));
  }

  /**
   * Convenience wrapper: retry a Flux once on authentication error by invoking the provided reset
   * action, then resubscribing.
   */
  public static <T> Flux<T> authRetry(
      final Flux<T> source, final Mono<Void> reset, final AuthErrorDetector detector) {
    return source.onErrorResume(
        t -> detector.isAuthError(t) ? reset.thenMany(source) : Mono.error(t));
  }

  /** Finds the first R2dbcException in a throwable cause chain. */
  private static R2dbcException findR2dbcException(final Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof R2dbcException ex) return ex;
      cur = cur.getCause();
    }
    return null;
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

    /** Returns true if the provided Throwable represents an authentication error. */
    boolean isAuthError(Throwable error);

    /** Combines this detector with another using OR semantics. */
    default AuthErrorDetector or(final AuthErrorDetector other) {
      return e -> this.isAuthError(e) || other.isAuthError(e);
    }
  }
}
