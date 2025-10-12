package com.example.rotatingdatasource.core;

import static com.example.rotatingdatasource.core.Retry.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RetryTest {

  @AfterEach
  void clearInterruptFlag() {
    if (Thread.currentThread().isInterrupted()) Thread.interrupted();
  }

  @Nested
  @DisplayName("Basic Retry Behavior - onException")
  class BasicRetryBehavior {

    @Test
    @DisplayName("Should succeed on first attempt")
    void shouldSucceedOnFirstAttempt() throws Exception {
      final var result = onException(() -> "success", ex -> true, () -> {}, 1, 0L);
      assertEquals("success", result);
    }

    @Test
    @DisplayName("Should retry on matching exception")
    void shouldRetryOnMatchingException() throws Exception {
      final var attempts = new AtomicInteger(0);

      final var result =
          onException(
              () -> {
                if (attempts.incrementAndGet() < 2) {
                  throw new SQLException("transient");
                }
                return "success";
              },
              ex -> true,
              () -> {},
              2,
              0L);

      assertEquals("success", result);
      assertEquals(2, attempts.get());
    }

    @Test
    @DisplayName("Should execute recovery action")
    void shouldExecuteRecoveryAction() throws Exception {
      final var recoveryExecuted = new boolean[] {false};
      final var attempts = new AtomicInteger(0);

      final var result =
          onException(
              () -> {
                if (attempts.incrementAndGet() < 2) {
                  throw new SQLException("transient");
                }
                return "recovered";
              },
              ex -> true,
              () -> recoveryExecuted[0] = true,
              2,
              0L);

      assertEquals("recovered", result);
      assertTrue(recoveryExecuted[0]);
    }

    @Test
    @DisplayName("Should fail after max attempts")
    void shouldFailAfterMaxAttempts() {
      final var attempts = new AtomicInteger(0);

      assertThrows(
          SQLException.class,
          () ->
              onException(
                  () -> {
                    attempts.incrementAndGet();
                    throw new SQLException("always fails");
                  },
                  ex -> true,
                  () -> {},
                  2,
                  0L));

      assertEquals(2, attempts.get());
    }
  }

  @Nested
  @DisplayName("Exception Filtering")
  class ExceptionFiltering {

    @Test
    @DisplayName("Should not retry on non-matching exception")
    void shouldNotRetryOnNonMatchingException() {
      assertThrows(
          SQLException.class,
          () ->
              onException(
                  () -> {
                    throw new SQLException("not retryable");
                  },
                  ex -> false,
                  () -> {},
                  3,
                  0L));
    }

    @Test
    @DisplayName("Should not retry when predicate returns false")
    void shouldNotRetryWhenPredicateReturnsFalse() {
      final var attempts = new AtomicInteger(0);

      assertThrows(
          SQLException.class,
          () ->
              onException(
                  () -> {
                    attempts.incrementAndGet();
                    throw new SQLException("08006");
                  },
                  ex -> false,
                  () -> {},
                  3,
                  0L));

      assertEquals(1, attempts.get());
    }
  }

  @Nested
  @DisplayName("Validation & Error Handling")
  class ValidationAndErrorHandling {

    @Test
    @DisplayName("Should throw on invalid max attempts")
    void onExceptionThrowsOnInvalidMaxAttempts() {
      assertThrows(
          IllegalArgumentException.class,
          () -> onException(() -> "test", ex -> true, () -> {}, 0, 0L));
    }

    @Test
    @DisplayName("Should re-interrupt and throw original exception on interrupted delay")
    void onExceptionInterruptedDuringDelayReinterruptsAndThrowsOriginal() {
      Thread.currentThread().interrupt();

      SQLException thrown =
          assertThrows(
              SQLException.class,
              () ->
                  onException(
                      () -> {
                        throw new SQLException("to retry");
                      },
                      ex -> true,
                      () -> {},
                      2,
                      1000L));

      assertEquals("to retry", thrown.getMessage());
      assertTrue(Thread.currentThread().isInterrupted());
    }
  }

  @Nested
  @DisplayName("Auth Error Detection")
  class AuthErrorDetection {

    @Test
    @DisplayName("Should detect auth error from SQL state 28000")
    void isAuthErrorTrueOnSqlState28000() {
      final var exception = new SQLException("anything", "28000");
      assertTrue(isAuthError(exception));
    }

    @Test
    @DisplayName("Should detect auth error from SQL state 28P01")
    void isAuthErrorTrueOnSqlState28P01() {
      final var exception = new SQLException("password failed", "28P01");
      assertTrue(isAuthError(exception));
    }

    @Test
    @DisplayName("Should detect auth error from common message keywords")
    void isAuthErrorTrueOnCommonMessageKeywords() {
      assertTrue(isAuthError(new SQLException("ACCESS DENIED for user")));
      assertTrue(isAuthError(new SQLException("authentication failed")));
      assertTrue(isAuthError(new SQLException("password authentication failed")));
      assertTrue(isAuthError(new SQLException("invalid password")));
      assertTrue(isAuthError(new SQLException("permission denied")));
    }

    @Test
    @DisplayName("Should return false when no auth indicators present")
    void isAuthErrorFalseWhenNoIndicators() {
      final var e = new SQLException("generic failure", "08006");
      assertFalse(isAuthError(e));
      assertFalse(isAuthError(null));
    }
  }

  @Nested
  @DisplayName("Transient Connection Error Detection")
  class TransientConnectionErrorDetection {

    @Test
    @DisplayName("Should detect transient error from SQLState 08xxx")
    void shouldDetectTransientFromSqlState08() {
      assertTrue(isTransientConnectionError(new SQLException("connection error", "08000")));
      assertTrue(isTransientConnectionError(new SQLException("connection error", "08006")));
    }

    @Test
    @DisplayName("Should detect SQLTransientException")
    void shouldDetectSqlTransientException() {
      assertTrue(isTransientConnectionError(new SQLTransientException("transient")));
    }

    @Test
    @DisplayName("Should detect JDBCConnectionException")
    void shouldDetectJdbcConnectionException() {
      final var exception = new TestJDBCConnectionException();
      assertTrue(isTransientConnectionError(exception));
    }

    @Test
    @DisplayName("Should detect transient keywords in message")
    void shouldDetectTransientKeywords() {
      assertTrue(isTransientConnectionError(new SQLException("connection refused")));
      assertTrue(isTransientConnectionError(new SQLException("connection reset")));
      assertTrue(isTransientConnectionError(new SQLException("i/o error occurred")));
      assertTrue(isTransientConnectionError(new SQLException("socket closed")));
      assertTrue(isTransientConnectionError(new SQLException("broken pipe")));
      assertTrue(isTransientConnectionError(new SQLException("pool exhausted")));
      assertTrue(isTransientConnectionError(new SQLException("connection timeout")));
    }

    @Test
    @DisplayName("Should return false for non-transient errors")
    void shouldReturnFalseForNonTransientErrors() {
      assertFalse(isTransientConnectionError(new SQLException("syntax error", "42000")));
      assertFalse(isTransientConnectionError(null));
    }

    // Helper class for testing
    static class TestJDBCConnectionException extends SQLException {
      TestJDBCConnectionException() {
        super("test");
      }
    }
  }

  @Nested
  @DisplayName("SQLException Extraction")
  class SqlExceptionExtraction {

    @Test
    @DisplayName("Should find first SQLException in exception chain")
    void findSqlExceptionReturnsFirstInChain() {
      final var inner = new SQLException("root", "28000");
      final var wrapped = new RuntimeException(new IllegalStateException(inner));
      assertEquals(inner, findSqlException(wrapped));
    }

    @Test
    @DisplayName("Should return null when no SQLException present")
    void findSqlExceptionReturnsNullWhenAbsent() {
      final var wrapped = new RuntimeException(new IllegalStateException("no sql here"));
      assertNull(findSqlException(wrapped));
    }

    @Test
    @DisplayName("Should find SQLException with chained next exceptions")
    void shouldFindLastNextException() {
      final var first = new SQLException("first");
      final var second = new SQLException("second");
      first.setNextException(second);
      final var wrapped = new RuntimeException(first);

      final var found = findSqlException(wrapped);
      assertEquals(first, found);
    }
  }

  @Nested
  @DisplayName("Retry Policy Tests")
  class PolicyTests {

    @Test
    @DisplayName("Fixed policy should return constant delay")
    void fixedPolicyShouldReturnConstantDelay() {
      final var policy = Policy.fixed(5, 100L);

      assertEquals(0L, policy.calculateDelay(1), "First attempt should have no delay");
      assertEquals(100L, policy.calculateDelay(2), "Second attempt should delay 100ms");
      assertEquals(100L, policy.calculateDelay(3), "Third attempt should delay 100ms");
      assertEquals(100L, policy.calculateDelay(4), "Fourth attempt should delay 100ms");
      assertEquals(100L, policy.calculateDelay(5), "Fifth attempt should delay 100ms");
    }

    @Test
    @DisplayName("Exponential policy should increase delay exponentially")
    void exponentialPolicyShouldIncreaseDelayExponentially() {
      final var policy = Policy.exponential(5, 100L);

      final var delay1 = policy.calculateDelay(1);
      final var delay2 = policy.calculateDelay(2);
      final var delay3 = policy.calculateDelay(3);
      final var delay4 = policy.calculateDelay(4);

      assertEquals(0L, delay1, "First attempt should have no delay");
      assertTrue(delay2 >= 100L && delay2 <= 125L, "Second delay should be ~100ms ± jitter");
      assertTrue(delay3 >= 200L && delay3 <= 250L, "Third delay should be ~200ms ± jitter");
      assertTrue(delay4 >= 400L && delay4 <= 500L, "Fourth delay should be ~400ms ± jitter");
    }

    @Test
    @DisplayName("Exponential policy should respect max delay cap")
    void exponentialPolicyShouldRespectMaxDelayCap() {
      final var policy = new Policy(10, 1000L, 5000L, 2.0, false);

      final var delay1 = policy.calculateDelay(1);
      final var delay5 = policy.calculateDelay(5);
      final var delay10 = policy.calculateDelay(10);

      assertEquals(0L, delay1);
      assertTrue(delay5 <= 5000L, "Delay should be capped at maxDelayMillis");
      assertTrue(delay10 <= 5000L, "Delay should be capped at maxDelayMillis");
    }

    @Test
    @DisplayName("Policy should validate constructor arguments")
    void policyShouldValidateConstructorArguments() {
      assertThrows(IllegalArgumentException.class, () -> new Policy(0, 100L, 100L, 1.0, false));

      assertThrows(IllegalArgumentException.class, () -> new Policy(1, -1L, 100L, 1.0, false));

      assertThrows(IllegalArgumentException.class, () -> new Policy(1, 200L, 100L, 1.0, false));

      assertThrows(IllegalArgumentException.class, () -> new Policy(1, 100L, 100L, 0.5, false));
    }
  }

  @Nested
  @DisplayName("Auth Retry with Reset Hook")
  class AuthRetryWithResetHook {

    @Test
    @DisplayName("Should succeed on first attempt without calling reset")
    void shouldSucceedOnFirstAttemptWithoutReset() {
      final var resetCalled = new AtomicInteger(0);

      final var result =
          authRetry(
              () -> "success", resetCalled::incrementAndGet, AuthErrorDetector.defaultDetector());

      assertEquals("success", result);
      assertEquals(0, resetCalled.get());
    }

    @Test
    @DisplayName("Should retry once on auth error after calling reset")
    void shouldRetryOnceOnAuthErrorAfterReset() {
      final var attempts = new AtomicInteger(0);
      final var resetCalled = new AtomicInteger(0);

      final var result =
          authRetry(
              () -> {
                if (attempts.incrementAndGet() == 1) {
                  throw new RuntimeException(new SQLException("auth failed", "28000"));
                }
                return "success";
              },
              resetCalled::incrementAndGet,
              AuthErrorDetector.defaultDetector());

      assertEquals("success", result);
      assertEquals(1, resetCalled.get());
    }

    @Test
    @DisplayName("Should throw RuntimeException on non-auth error")
    void shouldThrowOnNonAuthError() {
      final var resetCalled = new AtomicInteger(0);

      assertThrows(
          RuntimeException.class,
          () ->
              authRetry(
                  () -> {
                    throw new RuntimeException(new SQLException("syntax error", "42000"));
                  },
                  resetCalled::incrementAndGet,
                  AuthErrorDetector.defaultDetector()));

      assertEquals(0, resetCalled.get());
    }

    @Test
    @DisplayName("Should throw RuntimeException if retry fails after reset")
    void shouldThrowIfRetryFailsAfterReset() {
      final var resetCalled = new AtomicInteger(0);

      assertThrows(
          RuntimeException.class,
          () ->
              authRetry(
                  () -> {
                     throw new RuntimeException(new SQLException("auth failed", "28000"));
                  },
                  resetCalled::incrementAndGet,
                  AuthErrorDetector.defaultDetector()));

      assertEquals(1, resetCalled.get());
    }

    @Test
    @DisplayName("Should use custom detector")
    void shouldUseCustomDetector() {
      final var attempts = new AtomicInteger(0);
      final var resetCalled = new AtomicInteger(0);
      final var customDetector = AuthErrorDetector.custom(e -> e.getErrorCode() == 1017);

      final var result =
          authRetry(
              () -> {
                if (attempts.incrementAndGet() == 1) {
                  throw new RuntimeException(new SQLException("auth failed", null, 1017));
                }
                return "success";
              },
              resetCalled::incrementAndGet,
              customDetector);

      assertEquals("success", result);
      assertEquals(1, resetCalled.get());
    }
  }

  @Nested
  @DisplayName("Transient Retry with Policy")
  class TransientRetryWithPolicy {

    @Test
    @DisplayName("Should succeed on first attempt")
    void shouldSucceedOnFirstAttempt() {
      final var result = transientRetry(() -> "success", Policy.fixed(3, 0L));
      assertEquals("success", result);
    }

    @Test
    @DisplayName("Should retry on transient connection error")
    void shouldRetryOnTransientError() {
      final var attempts = new AtomicInteger(0);

      final var result =
          transientRetry(
              () -> {
                if (attempts.incrementAndGet() < 3) {
                  throw new RuntimeException(new SQLException("connection refused", "08006"));
                }
                return "success";
              },
              Policy.fixed(5, 0L));

      assertEquals("success", result);
      assertEquals(3, attempts.get());
    }

    @Test
    @DisplayName("Should throw on non-transient error")
    void shouldThrowOnNonTransientError() {
      assertThrows(
          RuntimeException.class,
          () ->
              transientRetry(
                  () -> {
                    throw new RuntimeException(new SQLException("syntax error", "42000"));
                  },
                  Policy.fixed(3, 0L)));
    }

    @Test
    @DisplayName("Should throw after max attempts exhausted")
    void shouldThrowAfterMaxAttempts() {
      final var attempts = new AtomicInteger(0);

      assertThrows(
          RuntimeException.class,
          () ->
              transientRetry(
                  () -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException(new SQLException("connection refused", "08006"));
                  },
                  Policy.fixed(3, 0L)));

      assertEquals(4, attempts.get());
    }
  }

  @Nested
  @DisplayName("AuthErrorDetector")
  class AuthErrorDetectorTests {

    @Test
    @DisplayName("Should combine detectors with OR logic")
    void shouldCombineDetectorsWithOr() {
      final var detector1 = AuthErrorDetector.custom(e -> e.getErrorCode() == 1017);
      final var detector2 = AuthErrorDetector.custom(e -> e.getErrorCode() == 1045);
      final var combined = detector1.or(detector2);

      assertTrue(combined.isAuthError(new SQLException("error", null, 1017)));
      assertTrue(combined.isAuthError(new SQLException("error", null, 1045)));
      assertFalse(combined.isAuthError(new SQLException("error", null, 9999)));
    }

    @Test
    @DisplayName("Default detector should use isAuthError method")
    void defaultDetectorShouldUseIsAuthError() {
      final var detector = AuthErrorDetector.defaultDetector();

      assertTrue(detector.isAuthError(new SQLException("auth failed", "28000")));
      assertFalse(detector.isAuthError(new SQLException("syntax error", "42000")));
    }
  }
}
