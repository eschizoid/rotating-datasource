package com.example.rotatingdatasource.core;

import static com.example.rotatingdatasource.core.Retry.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RetryTest {

  @AfterEach
  void clearInterruptFlag() {
    // Ensure one test's interrupt status doesn't leak into others
    if (Thread.currentThread().isInterrupted()) Thread.interrupted();
  }

  @Nested
  @DisplayName("Basic Retry Behavior")
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
      final var attempts = new int[] {0};

      final var result =
          onException(
              () -> {
                attempts[0]++;
                if (attempts[0] == 1) throw new SQLException("Connection failed");
                return "success";
              },
              ex -> true,
              () -> {},
              2,
              0L);

      assertEquals("success", result);
      assertEquals(2, attempts[0]);
    }

    @Test
    @DisplayName("Should execute recovery action")
    void shouldExecuteRecoveryAction() throws Exception {
      final var recoveryExecuted = new boolean[] {false};

      final var result =
          onException(
              () -> {
                if (!recoveryExecuted[0]) throw new SQLException("Auth failed");
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
      final var attempts = new int[] {0};

      assertThrows(
          SQLException.class,
          () ->
              onException(
                  () -> {
                    attempts[0]++;
                    throw new SQLException("Persistent failure");
                  },
                  ex -> true,
                  () -> {},
                  2,
                  0L));

      assertEquals(2, attempts[0]);
    }
  }

  @Nested
  @DisplayName("Exception Filtering")
  class ExceptionFiltering {

    @Test
    @DisplayName("Should not retry on non-matching exception")
    void shouldNotRetryOnNonMatchingException() {
      assertThrows(
          RuntimeException.class,
          () ->
              onException(
                  () -> {
                    throw new RuntimeException("Different exception type");
                  },
                  ex -> false,
                  () -> {},
                  2,
                  0L));
    }

    @Test
    @DisplayName("Should not retry when predicate returns false")
    void shouldNotRetryWhenPredicateReturnsFalse() {
      assertThrows(
          SQLTransientException.class,
          () ->
              onException(
                  () -> {
                    throw new SQLTransientException("Transient error");
                  },
                  ex -> false,
                  () -> {},
                  2,
                  0L));
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
          () -> onException(() -> "x", ex -> true, () -> {}, 0, 0L));
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
                      50L));

      assertEquals("to retry", thrown.getMessage());
      assertTrue(Thread.currentThread().isInterrupted());
    }
  }

  @Nested
  @DisplayName("Auth Retry Specific")
  class AuthRetrySpecific {

    @Test
    @DisplayName("Should retry once on auth SQL exception and call reset")
    void authRetryRetriesOnceOnAuthSQLExceptionAndCallsReset() {
      final var attempts = new int[] {0};
      final var resets = new int[] {0};

      final var result =
          authRetry(
              () -> {
                attempts[0]++;
                if (attempts[0] == 1) {
                  throw new RuntimeException(
                      new SQLException("Password authentication failed", "08000"));
                }
                return "ok";
              },
              () -> resets[0]++);

      assertEquals("ok", result);
      assertEquals(2, attempts[0]);
      assertEquals(1, resets[0]);
    }

    @Test
    @DisplayName("Should not retry on non-auth SQL exception")
    void authRetryDoesNotRetryOnNonAuthSQLException() {
      final var attempts = new int[] {0};
      final var resets = new int[] {0};

      RuntimeException thrown =
          assertThrows(
              RuntimeException.class,
              () ->
                  authRetry(
                      () -> {
                        attempts[0]++;
                        // SQLState not 28000 and no auth keywords
                        throw new RuntimeException(new SQLException("some other error", "08006"));
                      },
                      () -> resets[0]++));

      assertEquals(1, attempts[0]);
      assertEquals(0, resets[0]);
      assertInstanceOf(SQLException.class, thrown.getCause());
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
    @DisplayName("Should detect auth error from common message keywords")
    void isAuthErrorTrueOnCommonMessageKeywords() {
      final var exception = new SQLException("ACCESS DENIED for user");
      assertTrue(isAuthError(exception));
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
  @DisplayName("SQLException Extraction")
  class SqlExceptionExtraction {

    @Test
    @DisplayName("Should find first SQLException in exception chain")
    void findSqlExceptionReturnsFirstInChain() {
      final var inner = new SQLException("root", "28000");
      final var wrapped =
          new RuntimeException(new IllegalStateException(new RuntimeException(inner)));
      assertEquals(inner, findSqlException(wrapped));
    }

    @Test
    @DisplayName("Should return null when no SQLException present")
    void findSqlExceptionReturnsNullWhenAbsent() {
      final var wrapped = new RuntimeException(new IllegalStateException("no sql here"));
      assertNull(findSqlException(wrapped));
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
      assertThrows(
          IllegalArgumentException.class,
          () -> new Policy(0, 100L, 1000L, 2.0, false),
          "Should reject maxAttempts < 1");

      assertThrows(
          IllegalArgumentException.class,
          () -> new Policy(3, -1L, 1000L, 2.0, false),
          "Should reject negative initialDelayMillis");

      assertThrows(
          IllegalArgumentException.class,
          () -> new Policy(3, 1000L, 100L, 2.0, false),
          "Should reject maxDelayMillis < initialDelayMillis");

      assertThrows(
          IllegalArgumentException.class,
          () -> new Policy(3, 100L, 1000L, 0.5, false),
          "Should reject backoffMultiplier < 1.0");
    }
  }

  @Nested
  @DisplayName("Auth Retry with RotatingDataSource")
  class AuthRetryWithRotatingDataSource {

    @Test
    void testAuthRetryWithRotatingDataSource() {
      var rotatingDs = mock(RotatingDataSource.class);
      var callCount = new AtomicInteger(0);

      Supplier<String> supplier =
          () -> {
            if (callCount.incrementAndGet() == 1) {
              throw new RuntimeException(new SQLException("Access denied", "28000"));
            }
            return "success";
          };

      final var result = Retry.authRetry(supplier, rotatingDs);

      assertEquals("success", result);
      verify(rotatingDs, times(1)).reset();
    }

    @Test
    void testAuthRetryWithRotatingDataSourceNoAuthError() {
      var rotatingDs = mock(RotatingDataSource.class);

      Supplier<String> supplier = () -> "success";

      final var result = Retry.authRetry(supplier, rotatingDs);

      assertEquals("success", result);
      verify(rotatingDs, never()).reset();
    }

    @Test
    void testAuthRetryWithRotatingDataSourceNonAuthError() {
      var rotatingDs = mock(RotatingDataSource.class);

      Supplier<String> supplier =
          () -> {
            throw new RuntimeException("Some other error");
          };

      assertThrows(RuntimeException.class, () -> Retry.authRetry(supplier, rotatingDs));
      verify(rotatingDs, never()).reset();
    }

    @Test
    void testAuthRetryWithRotatingDataSourceAndDetector() {
      var rotatingDs = mock(RotatingDataSource.class);
      var detector = Retry.AuthErrorDetector.custom(e -> e.getErrorCode() == 1017);
      var callCount = new AtomicInteger(0);

      Supplier<String> supplier =
          () -> {
            if (callCount.incrementAndGet() == 1) {
              var sqlEx = new SQLException("Invalid password", "28000", 1017);
              throw new RuntimeException(sqlEx);
            }
            return "success";
          };

      final var result = Retry.authRetry(supplier, rotatingDs, detector);

      assertEquals("success", result);
      verify(rotatingDs, times(1)).reset();
    }

    @Test
    void testAuthRetryWithRotatingDataSourceAndDetectorNoMatch() {
      final var rotatingDs = mock(RotatingDataSource.class);
      final var detector = Retry.AuthErrorDetector.custom(e -> e.getErrorCode() == 1017);

      Supplier<String> supplier =
          () -> {
            final var sqlEx = new SQLException("Invalid password", "28000", 9999);
            throw new RuntimeException(sqlEx);
          };

      assertThrows(RuntimeException.class, () -> Retry.authRetry(supplier, rotatingDs, detector));
      verify(rotatingDs, never()).reset();
    }

    @Test
    void testAuthRetryWithExpectedType() {
      final var rotatingDs = mock(RotatingDataSource.class);
      final var callCount = new AtomicInteger(0);

      Supplier<Object> supplier =
          () -> {
            if (callCount.incrementAndGet() == 1) {
              throw new RuntimeException(new SQLException("Access denied", "28000"));
            }
            return 42;
          };

      Integer result = Retry.authRetry(supplier, rotatingDs, Integer.class);

      assertEquals(42, result);
      verify(rotatingDs, times(1)).reset();
    }

    @Test
    void testAuthRetryWithExpectedTypeClassCastException() {
      final var rotatingDs = mock(RotatingDataSource.class);

      Supplier<Object> supplier = () -> "not an integer";

      assertThrows(
          ClassCastException.class, () -> Retry.authRetry(supplier, rotatingDs, Integer.class));
    }

    @Test
    void testAuthRetryVoidOperation() {
      final var rotatingDs = mock(RotatingDataSource.class);
      final var callCount = new AtomicInteger(0);
      final var executed = new AtomicInteger(0);

      Runnable operation =
          () -> {
            if (callCount.incrementAndGet() == 1) {
              throw new RuntimeException(new SQLException("Access denied", "28000"));
            }
            executed.incrementAndGet();
          };

      Retry.authRetry(operation, rotatingDs);

      assertEquals(1, executed.get());
      verify(rotatingDs, times(1)).reset();
    }

    @Test
    void testAuthRetryVoidOperationNoAuthError() {
      final var rotatingDs = mock(RotatingDataSource.class);
      final var executed = new AtomicInteger(0);

      Runnable operation = executed::incrementAndGet;

      Retry.authRetry(operation, rotatingDs);

      assertEquals(1, executed.get());
      verify(rotatingDs, never()).reset();
    }

    @Test
    void testAuthRetryWithRunnableAndDetector() throws SQLException {
      final var rotatingDs = mock(RotatingDataSource.class);
      final var callCount = new AtomicInteger(0);
      final var executed = new AtomicInteger(0);

      Runnable operation =
          () -> {
            if (callCount.incrementAndGet() == 1) {
              throw new RuntimeException(new SQLException("Password authentication failed"));
            }
            executed.incrementAndGet();
          };

      Retry.authRetry(operation, rotatingDs);

      assertEquals(1, executed.get());
      verify(rotatingDs, times(1)).reset();
    }
  }

  @Nested
  @DisplayName("Retry with Listener")
  class RetryWithListener {

    @Test
    void testOnExceptionWithRetryListener() throws SQLException {
      final var listener = mock(Retry.RetryListener.class);
      final var callCount = new AtomicInteger(0);

      Retry.SqlExceptionSupplier<String, SQLException> supplier =
          () -> {
            if (callCount.incrementAndGet() < 3) {
              throw new SQLException("Transient error");
            }
            return "success";
          };

      String result = Retry.onException(supplier, e -> true, () -> {}, 3, 10L, listener);

      assertEquals("success", result);
      verify(listener, times(2)).onRetry(anyInt(), any(SQLException.class));
    }

    @Test
    void testOnExceptionWithRetryListenerFailure() {
      final var listener = mock(Retry.RetryListener.class);
      final var callCount = new AtomicInteger(0);

      Retry.SqlExceptionSupplier<String, SQLException> supplier =
          () -> {
            callCount.incrementAndGet();
            throw new SQLException("Permanent error");
          };

      assertThrows(
          SQLException.class,
          () -> Retry.onException(supplier, e -> true, () -> {}, 2, 10L, listener));
      verify(listener, times(2)).onRetry(anyInt(), any(SQLException.class));
    }

    @Test
    void testOnExceptionWithRetryListenerNoRetry() throws SQLException {
      final var listener = mock(Retry.RetryListener.class);

      Retry.SqlExceptionSupplier<String, SQLException> supplier = () -> "success";

      String result = Retry.onException(supplier, e -> true, () -> {}, 3, 10L, listener);

      assertEquals("success", result);
      verify(listener, never()).onRetry(anyInt(), any());
    }

    @Test
    void testRetryListenerLogging() throws SQLException {
      final var listener = Retry.RetryListener.logging();
      final var callCount = new AtomicInteger(0);

      Retry.SqlExceptionSupplier<String, SQLException> supplier =
          () -> {
            if (callCount.incrementAndGet() < 2) {
              throw new SQLException("Transient error");
            }
            return "success";
          };

      // Should not throw, logging is just for observability
      final var result = Retry.onException(supplier, e -> true, () -> {}, 2, 10L, listener);
      assertEquals("success", result);
    }

    @Test
    void testRetryListenerNoOp() throws SQLException {
      final var listener = Retry.RetryListener.noOp();
      final var callCount = new AtomicInteger(0);

      Retry.SqlExceptionSupplier<String, SQLException> supplier =
          () -> {
            if (callCount.incrementAndGet() < 2) {
              throw new SQLException("Transient error");
            }
            return "success";
          };

      String result = Retry.onException(supplier, e -> true, () -> {}, 2, 10L, listener);
      assertEquals("success", result);
    }
  }
}
