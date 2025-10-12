package com.example.rotatingdatasource.core;

import io.r2dbc.spi.R2dbcException;
import java.util.Locale;

final class R2dbcRetryHelper {

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

  static boolean isAuthError(Throwable error) {
    if (!(error instanceof R2dbcException r2dbcEx)) {
      return false;
    }

    final var sqlState = r2dbcEx.getSqlState();
    if ("28000".equals(sqlState) || "28P01".equals(sqlState)) {
      return true;
    }

    final var msg = r2dbcEx.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (var keyword : AUTH_KEYWORDS) {
        if (lower.contains(keyword)) return true;
      }
    }

    return false;
  }

  static boolean isTransientConnectionError(Throwable error) {
    if (!(error instanceof R2dbcException r2dbcEx)) {
      return false;
    }

    final var sqlState = r2dbcEx.getSqlState();
    if (sqlState != null && sqlState.startsWith("08")) {
      return true;
    }

    final var msg = r2dbcEx.getMessage();
    if (msg != null) {
      final var lower = msg.toLowerCase(Locale.ROOT);
      for (var keyword : TRANSIENT_KEYWORDS) {
        if (lower.contains(keyword)) return true;
      }
    }

    return false;
  }
}
