package com.example.rotatingdatasource.core.reactive;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
}
