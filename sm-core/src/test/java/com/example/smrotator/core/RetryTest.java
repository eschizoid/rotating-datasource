package com.example.smrotator.core;

import static com.example.smrotator.core.Retry.*;
import static com.example.smrotator.core.Retry.onException;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class RetryTest {

  @AfterEach
  void clearInterruptFlag() {
    // Ensure one test's interrupt status doesn't leak into others
    if (Thread.currentThread().isInterrupted()) Thread.interrupted();
  }

  @Test
  void shouldSucceedOnFirstAttempt() throws Exception {
    final var result = onException(() -> "success", ex -> true, () -> {}, 1, 0L);
    assertEquals("success", result);
  }

  @Test
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

  @Test
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

  @Test
  void onExceptionThrowsOnInvalidMaxAttempts() {
    assertThrows(
        IllegalArgumentException.class, () -> onException(() -> "x", ex -> true, () -> {}, 0, 0L));
  }

  @Test
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

  @Test
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

  @Test
  void isAuthErrorTrueOnSqlState28000() {
    final var e = new SQLException("anything", "28000");
    assertTrue(isAuthError(e));
  }

  @Test
  void isAuthErrorTrueOnCommonMessageKeywords() {
    final var e = new SQLException("ACCESS DENIED for user bob");
    assertTrue(isAuthError(e));
  }

  @Test
  void isAuthErrorFalseWhenNoIndicators() {
    final var e = new SQLException("generic failure", "08006");
    assertFalse(isAuthError(e));
    assertFalse(isAuthError(null));
  }

  @Test
  void findSqlExceptionReturnsFirstInChain() {
    final var inner = new SQLException("root", "28000");
    final var wrapped =
        new RuntimeException(new IllegalStateException(new RuntimeException(inner)));
    assertEquals(inner, findSqlException(wrapped));
  }

  @Test
  void findSqlExceptionReturnsNullWhenAbsent() {
    final var wrapped = new RuntimeException(new IllegalStateException("no sql here"));
    assertNull(findSqlException(wrapped));
  }
}
