package com.example.rotatingdatasource.core;

import java.util.function.Predicate;

@FunctionalInterface
public interface R2dbcAuthErrorDetector {

  boolean isAuthError(Throwable error);

  static R2dbcAuthErrorDetector defaultDetector() {
    return R2dbcRetryHelper::isAuthError;
  }

  static R2dbcAuthErrorDetector custom(final Predicate<Throwable> predicate) {
    return predicate::test;
  }

  default R2dbcAuthErrorDetector or(R2dbcAuthErrorDetector other) {
    return e -> this.isAuthError(e) || other.isAuthError(e);
  }
}
