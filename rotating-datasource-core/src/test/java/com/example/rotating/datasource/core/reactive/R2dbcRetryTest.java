package com.example.rotating.datasource.core.reactive;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class R2dbcRetryTest {

  @Test
  @DisplayName("Detects auth errors by SQLSTATE 28P01 and 28000 and by message keywords")
  void shouldDetectAuthErrors() {
    final var byState1 = new R2dbcNonTransientResourceException("bad creds", "28P01", 0);
    final var byState2 = new R2dbcNonTransientResourceException("bad creds", "28000", 0);
    final var byMsg =
        new R2dbcNonTransientResourceException("Password authentication failed for user", null, 0);
    final var notAuth = new R2dbcNonTransientResourceException("syntax error", "42601", 0);

    assertTrue(R2dbcRetry.isAuthError(byState1));
    assertTrue(R2dbcRetry.isAuthError(byState2));
    assertTrue(R2dbcRetry.isAuthError(byMsg));
    assertFalse(R2dbcRetry.isAuthError(notAuth));
  }

  @Test
  @DisplayName("Detects transient connection errors by SQLSTATE class 08 and keywords")
  void shouldDetectTransientErrors() {
    final var byState = new R2dbcTransientResourceException("connection dropped", "08006", 0);
    final var byMsg = new R2dbcTransientResourceException("Connection refused", null, 0);
    final var stable = new R2dbcNonTransientResourceException("unique violation", "23505", 0);

    assertTrue(R2dbcRetry.isTransientConnectionError(byState));
    assertTrue(R2dbcRetry.isTransientConnectionError(byMsg));
    assertFalse(R2dbcRetry.isTransientConnectionError(stable));
  }

  @Test
  @DisplayName("AuthErrorDetector default/custom and OR-composition work")
  void detectorComposition() {
    final var defaultDetector = R2dbcRetry.AuthErrorDetector.defaultDetector();
    final var custom =
        R2dbcRetry.AuthErrorDetector.custom(
            t -> t.getMessage() != null && t.getMessage().contains("custom-auth"));

    final var combined = defaultDetector.or(custom);

    assertTrue(
        defaultDetector.isAuthError(
            new R2dbcNonTransientResourceException("Password authentication failed", null, 0)));
    assertTrue(custom.isAuthError(new RuntimeException("custom-auth marker")));
    assertTrue(combined.isAuthError(new RuntimeException("custom-auth marker")));
    assertFalse(defaultDetector.isAuthError(new RuntimeException("other")));
  }

  @Test
  @DisplayName("authRetry(Mono) retries once on auth error and invokes reset exactly once")
  void authRetryMonoRetriesOnceOnAuthError() {
    final var attempts = new AtomicInteger(0);
    final var resets = new AtomicInteger(0);

    final Mono<String> source =
        Mono.defer(
            () -> {
              if (attempts.getAndIncrement() == 0) {
                return Mono.error(
                    new R2dbcNonTransientResourceException(
                        "Password authentication failed", "28P01", 0));
              }
              return Mono.just("ok");
            });

    final var result =
        R2dbcRetry.authRetry(
                source,
                Mono.fromRunnable(resets::incrementAndGet),
                R2dbcRetry.AuthErrorDetector.defaultDetector())
            .block();

    assertEquals("ok", result);
    assertEquals(2, attempts.get(), "should subscribe twice (one retry)");
    assertEquals(1, resets.get(), "reset should be called once");
  }

  @Test
  @DisplayName("authRetry(Mono) does not reset on non-auth error and propagates")
  void authRetryMonoDoesNotResetOnNonAuth() {
    final var resets = new AtomicInteger(0);

    final Mono<String> source =
        Mono.defer(
            () -> Mono.error(new R2dbcNonTransientResourceException("syntax error", "42601", 0)));

    final var mono =
        R2dbcRetry.authRetry(
            source,
            Mono.fromRunnable(resets::incrementAndGet),
            R2dbcRetry.AuthErrorDetector.defaultDetector());

    try {
      mono.block();
      fail("Expected exception");
    } catch (Throwable t) {
      // expected
    }
    assertEquals(0, resets.get(), "reset should not be called for non-auth errors");
  }

  @Test
  @DisplayName("fixed(attempts, delay) results in expected resubscriptions")
  void fixedRetryProducesExpectedResubscriptions() {
    final var attempts = new AtomicInteger(0);
    final int failTimes = 2; // First 2 attempts fail, then succeed on 3rd

    final Mono<String> source =
        Mono.defer(
                () -> {
                  final int a = attempts.incrementAndGet();
                  if (a <= failTimes) {
                    return Mono.error(
                        new R2dbcTransientResourceException("conn reset", "08006", 0));
                  }
                  return Mono.just("done");
                })
            .retryWhen(R2dbcRetry.fixed(failTimes + 1, Duration.ofMillis(1)));

    final var out = source.block();
    assertEquals("done", out);
    assertEquals(failTimes + 1, attempts.get(), "should attempt initial + retries");
  }

  @Test
  @DisplayName("toReactorRetry(Policy.fixed) adapts to expected number of retries")
  void toReactorRetryAdaptsFixedPolicy() {
    final var attempts = new AtomicInteger(0);
    final int totalAttempts = 3; // initial + 2 retries

    final Mono<String> source =
        Mono.defer(
                () -> {
                  final int a = attempts.incrementAndGet();
                  if (a < totalAttempts) {
                    return Mono.error(new R2dbcTransientResourceException("temporary", "08006", 0));
                  }
                  return Mono.just("ok");
                })
            .retryWhen(
                R2dbcRetry.toReactorRetry(
                    com.example.rotating.datasource.core.jdbc.Retry.Policy.fixed(
                        totalAttempts, 1)));

    final var out = source.block();
    assertEquals("ok", out);
    assertEquals(totalAttempts, attempts.get());
  }

  @Test
  @DisplayName("transientErrorPredicate matches transient exceptions")
  void transientPredicateMatches() {
    assertTrue(
        R2dbcRetry.transientErrorPredicate()
            .test(new R2dbcTransientResourceException("conn refused", "08001", 0)));
    assertFalse(
        R2dbcRetry.transientErrorPredicate()
            .test(new R2dbcNonTransientResourceException("unique violation", "23505", 0)));
  }
}
