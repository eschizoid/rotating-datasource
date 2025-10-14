package com.example.rotatingdatasource.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RotatingDataSourceTest {

  private DataSourceFactory mockFactory;
  private DataSource mockDataSource;
  private MockedStatic<SecretHelper> secretHelperMock;

  @BeforeEach
  void setUp() {
    mockFactory = mock(DataSourceFactory.class);
    mockDataSource = mock(DataSource.class);
    when(mockFactory.create(any())).thenReturn(mockDataSource);

    // Mock SecretHelper static methods
    secretHelperMock = mockStatic(SecretHelper.class);
    secretHelperMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("version-1");
    secretHelperMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(new DbSecret("user", "password", "postgres", "localhost", 5432, "testdb"));
  }

  @AfterEach
  void tearDown() throws Exception {
    if (secretHelperMock != null) {
      secretHelperMock.close();
    }
    if (Thread.currentThread().isInterrupted()) {
      Thread.interrupted();
    }
  }

  @Nested
  @DisplayName("Builder Validation")
  class BuilderValidation {

    @Test
    @DisplayName("Should throw when secretId is null")
    void shouldThrowWhenSecretIdIsNull() {
      assertThrows(
          IllegalStateException.class,
          () -> RotatingDataSource.builder().secretId(null).factory(mockFactory).build());
    }

    @Test
    @DisplayName("Should throw when secretId is blank")
    void shouldThrowWhenSecretIdIsBlank() {
      assertThrows(
          IllegalStateException.class,
          () -> RotatingDataSource.builder().secretId("").factory(mockFactory).build());
    }

    @Test
    @DisplayName("Should throw when factory is null")
    void shouldThrowWhenFactoryIsNull() {
      assertThrows(
          IllegalStateException.class,
          () -> RotatingDataSource.builder().secretId("test-secret").factory(null).build());
    }

    @Test
    @DisplayName("Should throw when refreshIntervalSeconds is negative")
    void shouldThrowWhenRefreshIntervalIsNegative() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              RotatingDataSource.builder()
                  .secretId("test-secret")
                  .factory(mockFactory)
                  .refreshIntervalSeconds(-1L)
                  .build());
    }

    @Test
    @DisplayName("Should throw when overlapDuration is negative")
    void shouldThrowWhenOverlapDurationIsNegative() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              RotatingDataSource.builder()
                  .secretId("test-secret")
                  .factory(mockFactory)
                  .overlapDuration(Duration.ofSeconds(-1))
                  .build());
    }

    @Test
    @DisplayName("Should throw when gracePeriod is negative")
    void shouldThrowWhenGracePeriodIsNegative() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              RotatingDataSource.builder()
                  .secretId("test-secret")
                  .factory(mockFactory)
                  .gracePeriod(Duration.ofSeconds(-1))
                  .build());
    }

    @Test
    @DisplayName("Should accept zero refresh interval")
    void shouldAcceptZeroRefreshInterval() {
      assertDoesNotThrow(
          () ->
              RotatingDataSource.builder()
                  .secretId("test-secret")
                  .factory(mockFactory)
                  .refreshIntervalSeconds(0L)
                  .build());
    }

    @Test
    @DisplayName("Should accept zero overlap duration")
    void shouldAcceptZeroOverlapDuration() {
      assertDoesNotThrow(
          () ->
              RotatingDataSource.builder()
                  .secretId("test-secret")
                  .factory(mockFactory)
                  .overlapDuration(Duration.ZERO)
                  .build());
    }
  }

  @Nested
  @DisplayName("Basic Connection Operations")
  class BasicConnectionOperations {

    @Test
    @DisplayName("Should get connection from primary DataSource")
    void shouldGetConnectionFromPrimaryDataSource() throws SQLException {
      final var mockConnection = mock(Connection.class);
      when(mockDataSource.getConnection()).thenReturn(mockConnection);

      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      final var connection = rotatingDs.getConnection();

      assertNotNull(connection);
      verify(mockDataSource, times(1)).getConnection();
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for getConnection with credentials")
    void shouldThrowUnsupportedOperationForGetConnectionWithCredentials() {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      assertThrows(
          UnsupportedOperationException.class, () -> rotatingDs.getConnection("user", "pass"));
    }

    @Test
    @DisplayName("Should propagate SQLException from DataSource")
    void shouldPropagateSqlException() throws SQLException {
      when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      assertThrows(RuntimeException.class, rotatingDs::getConnection);
    }
  }

  @Nested
  @DisplayName("Credential Rotation")
  class CredentialRotation {

    @Test
    @DisplayName("Should rotate credentials on reset")
    void shouldRotateCredentialsOnReset() throws SQLException, InterruptedException {
      final var oldDataSource = mock(DataSource.class);
      final var newDataSource = mock(DataSource.class);
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .gracePeriod(Duration.ofMillis(100))
              .build();

      rotatingDs.reset();

      Thread.sleep(150);

      final var newConnection = mock(Connection.class);
      when(newDataSource.getConnection()).thenReturn(newConnection);

      final var connection = rotatingDs.getConnection();
      assertNotNull(connection);

      verify(factory, times(2)).create(any());
    }

    @Test
    @DisplayName("Should handle concurrent reset calls")
    void shouldHandleConcurrentResetCalls() throws InterruptedException {
      final var resetCount = new AtomicInteger(0);
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any()))
          .thenAnswer(
              inv -> {
                resetCount.incrementAndGet();
                return mockDataSource;
              });

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .gracePeriod(Duration.ofMillis(50))
              .build();

      final var latch = new CountDownLatch(5);
      for (int i = 0; i < 5; i++) {
        new Thread(
                () -> {
                  try {
                    rotatingDs.reset();
                  } finally {
                    latch.countDown();
                  }
                })
            .start();
      }

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      assertTrue(resetCount.get() >= 1 && resetCount.get() <= 6);
    }

    @Test
    @DisplayName("Should fail reset when factory throws exception")
    void shouldFailResetWhenFactoryThrows() {
      final var factory = mock(DataSourceFactory.class);
      when(factory.create(any()))
          .thenReturn(mockDataSource)
          .thenThrow(new RuntimeException("Factory failed"));

      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(factory).build();

      assertThrows(RuntimeException.class, rotatingDs::reset);
    }
  }

  @Nested
  @DisplayName("Dual Password Support")
  class DualPasswordSupport {

    @Test
    @DisplayName("Should maintain secondary DataSource during overlap period")
    void shouldMaintainSecondaryDataSourceDuringOverlap() throws InterruptedException {
      final var oldDataSource = mock(DataSource.class);
      final var newDataSource = mock(DataSource.class);
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .overlapDuration(Duration.ofSeconds(1))
              .build();

      rotatingDs.reset();

      assertTrue(rotatingDs.isOverlapActive());
      assertTrue(rotatingDs.getOverlapExpiresAt().isPresent());

      Thread.sleep(1100);

      assertFalse(rotatingDs.isOverlapActive());
    }

    @Test
    @DisplayName("Should fallback to secondary DataSource on auth error during overlap")
    void shouldFallbackToSecondaryOnAuthError() throws SQLException {
      final var oldDataSource = mock(DataSource.class);
      final var newDataSource = mock(DataSource.class);
      final var secondaryConnection = mock(Connection.class);
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      when(newDataSource.getConnection()).thenThrow(new SQLException("auth failed", "28000"));
      when(oldDataSource.getConnection()).thenReturn(secondaryConnection);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .overlapDuration(Duration.ofSeconds(10))
              .build();

      rotatingDs.reset();

      final var connection = rotatingDs.getConnection();
      assertNotNull(connection);
      verify(oldDataSource, atLeastOnce()).getConnection();
    }

    @Test
    @DisplayName("Should not activate overlap when duration is zero")
    void shouldNotActivateOverlapWhenDurationIsZero() {
      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(mockFactory)
              .overlapDuration(Duration.ZERO)
              .build();

      rotatingDs.reset();

      assertFalse(rotatingDs.isOverlapActive());
      assertTrue(rotatingDs.getOverlapExpiresAt().isEmpty());
    }

    @Test
    @DisplayName("Should expire secondary DataSource after overlap period")
    void shouldExpireSecondaryAfterOverlap() throws InterruptedException {
      final var oldDataSource = mock(DataSource.class);
      final var newDataSource = mock(DataSource.class);
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .overlapDuration(Duration.ofMillis(200))
              .build();

      rotatingDs.reset();
      assertTrue(rotatingDs.isOverlapActive());

      Thread.sleep(250);

      assertFalse(rotatingDs.isOverlapActive());
    }
  }

  @Nested
  @DisplayName("Grace Period")
  class GracePeriod {

    @Test
    @Disabled
    @DisplayName("Should close old DataSource after grace period")
    void shouldCloseOldDataSourceAfterGracePeriod() throws Exception {
      final var oldDataSource =
          mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
      final var newDataSource = mock(DataSource.class);
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .gracePeriod(Duration.ofMillis(100))
              .build();

      rotatingDs.reset();

      Thread.sleep(150);

      verify((AutoCloseable) oldDataSource, times(1)).close();
    }

    @Test
    @DisplayName("Should not close DataSource during grace period")
    void shouldNotCloseDataSourceDuringGracePeriod() throws Exception {
      final var oldDataSource =
          mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
      final var newDataSource = mock(DataSource.class);
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .gracePeriod(Duration.ofSeconds(5))
              .build();

      rotatingDs.reset();

      Thread.sleep(100);

      verify((AutoCloseable) oldDataSource, never()).close();
    }

    @Test
    @DisplayName("Should cancel pending grace period on new rotation")
    void shouldCancelPendingGracePeriodOnNewRotation() throws Exception {
      final var oldDataSource =
          mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
      final var newDataSource =
          mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
      final var newerDataSource = mock(DataSource.class);
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any()))
          .thenReturn(oldDataSource)
          .thenReturn(newDataSource)
          .thenReturn(newerDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .gracePeriod(Duration.ofSeconds(5))
              .build();

      rotatingDs.reset();
      Thread.sleep(50);
      rotatingDs.reset();

      Thread.sleep(100);

      verify((AutoCloseable) oldDataSource, atMost(1)).close();
    }
  }

  @Nested
  @DisplayName("Proactive Refresh")
  class ProactiveRefresh {

    @Test
    @Disabled
    @DisplayName("Should schedule proactive refresh when interval is set")
    void shouldScheduleProactiveRefresh() throws InterruptedException {
      final var factory = mock(DataSourceFactory.class);
      final var oldDataSource = mock(DataSource.class);
      final var newDataSource = mock(DataSource.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .refreshIntervalSeconds(1L)
              .build();

      Thread.sleep(1500);

      verify(factory, atLeast(2)).create(any());

      rotatingDs.shutdown();
    }

    @Test
    @DisplayName("Should not schedule refresh when interval is zero")
    void shouldNotScheduleRefreshWhenIntervalIsZero() throws InterruptedException {
      final var factory = mock(DataSourceFactory.class);
      when(factory.create(any())).thenReturn(mockDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .refreshIntervalSeconds(0L)
              .build();

      Thread.sleep(500);

      verify(factory, times(1)).create(any());
    }
  }

  @Nested
  @DisplayName("DataSource Wrapper Methods")
  class DataSourceWrapperMethods {

    @Test
    @DisplayName("Should delegate getLogWriter to primary DataSource")
    void shouldDelegateGetLogWriter() throws SQLException {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      rotatingDs.getLogWriter();

      verify(mockDataSource, times(1)).getLogWriter();
    }

    @Test
    @DisplayName("Should delegate setLogWriter to primary DataSource")
    void shouldDelegateSetLogWriter() throws SQLException {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      rotatingDs.setLogWriter(null);

      verify(mockDataSource, times(1)).setLogWriter(null);
    }

    @Test
    @DisplayName("Should delegate setLoginTimeout to primary DataSource")
    void shouldDelegateSetLoginTimeout() throws SQLException {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      rotatingDs.setLoginTimeout(30);

      verify(mockDataSource, times(1)).setLoginTimeout(30);
    }

    @Test
    @DisplayName("Should delegate getLoginTimeout to primary DataSource")
    void shouldDelegateGetLoginTimeout() throws SQLException {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      rotatingDs.getLoginTimeout();

      verify(mockDataSource, times(1)).getLoginTimeout();
    }

    @Test
    @DisplayName("Should unwrap to itself if class matches")
    void shouldUnwrapToItself() throws SQLException {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      final var unwrapped = rotatingDs.unwrap(RotatingDataSource.class);

      assertSame(rotatingDs, unwrapped);
    }

    @Test
    @Disabled
    @DisplayName("Should delegate unwrap to primary DataSource")
    void shouldDelegateUnwrap() throws SQLException {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      rotatingDs.unwrap(DataSource.class);

      verify(mockDataSource, times(1)).unwrap(DataSource.class);
    }

    @Test
    @DisplayName("Should return true for isWrapperFor itself")
    void shouldReturnTrueForIsWrapperForItself() throws SQLException {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      assertTrue(rotatingDs.isWrapperFor(RotatingDataSource.class));
    }

    @Test
    @Disabled
    @DisplayName("Should delegate isWrapperFor to primary DataSource")
    void shouldDelegateIsWrapperFor() throws SQLException {
      final var rotatingDs =
          RotatingDataSource.builder().secretId("test-secret").factory(mockFactory).build();

      rotatingDs.isWrapperFor(DataSource.class);

      verify(mockDataSource, times(1)).isWrapperFor(DataSource.class);
    }
  }

  @Nested
  @DisplayName("Shutdown")
  class Shutdown {

    @Test
    @DisplayName("Should close all DataSources on shutdown")
    void shouldCloseAllDataSourcesOnShutdown() throws Exception {
      final var primaryDataSource =
          mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
      final var secondaryDataSource =
          mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
      final var factory = mock(DataSourceFactory.class);

      when(factory.create(any())).thenReturn(primaryDataSource).thenReturn(secondaryDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .overlapDuration(Duration.ofSeconds(10))
              .build();

      rotatingDs.reset();
      Thread.sleep(50);

      rotatingDs.shutdown();

      verify((AutoCloseable) primaryDataSource, times(1)).close();
      verify((AutoCloseable) secondaryDataSource, times(1)).close();
    }

    @Test
    @DisplayName("Should shutdown scheduler on shutdown")
    void shouldShutdownSchedulerOnShutdown() throws InterruptedException {
      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(mockFactory)
              .refreshIntervalSeconds(1L)
              .build();

      rotatingDs.shutdown();

      Thread.sleep(1500);

      verify(mockFactory, times(1)).create(any());
    }

    @Test
    @DisplayName("Should cancel pending cleanups on shutdown")
    void shouldCancelPendingCleanupsOnShutdown() {
      final var factory = mock(DataSourceFactory.class);
      final var oldDataSource = mock(DataSource.class);
      final var newDataSource = mock(DataSource.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .overlapDuration(Duration.ofSeconds(10))
              .build();

      rotatingDs.reset();
      rotatingDs.shutdown();

      assertFalse(rotatingDs.isOverlapActive());
    }
  }

  @Nested
  @DisplayName("Custom Retry Policy")
  class CustomRetryPolicy {

    @Test
    @DisplayName("Should use custom retry policy")
    void shouldUseCustomRetryPolicy() throws SQLException {
      final var attempts = new AtomicInteger(0);
      final var factory = mock(DataSourceFactory.class);
      final var dataSource = mock(DataSource.class);

      when(factory.create(any())).thenReturn(dataSource);
      when(dataSource.getConnection())
          .thenAnswer(
              inv -> {
                if (attempts.getAndIncrement() < 2) {
                  throw new SQLException("auth failed", "28000");
                }
                return mock(Connection.class);
              });

      final var policy = Retry.Policy.fixed(3, 10L);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .retryPolicy(policy)
              .build();

      final var connection = rotatingDs.getConnection();

      assertNotNull(connection);
      assertTrue(attempts.get() >= 2);
    }
  }

  @Nested
  @DisplayName("Custom Auth Error Detector")
  class CustomAuthErrorDetector {

    @Test
    @DisplayName("Should use custom auth error detector")
    void shouldUseCustomAuthErrorDetector() throws SQLException {
      final var attempts = new AtomicInteger(0);
      final var factory = mock(DataSourceFactory.class);
      final var oldDataSource = mock(DataSource.class);
      final var newDataSource = mock(DataSource.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      when(oldDataSource.getConnection())
          .thenAnswer(
              inv -> {
                if (attempts.getAndIncrement() == 0) {
                  throw new SQLException("custom auth error", null, 1017);
                }
                return mock(Connection.class);
              });

      when(newDataSource.getConnection()).thenReturn(mock(Connection.class));

      final var customDetector = Retry.AuthErrorDetector.custom(e -> e.getErrorCode() == 1017);

      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .authErrorDetector(customDetector)
              .build();

      final var connection = rotatingDs.getConnection();

      assertNotNull(connection);
      verify(factory, times(2)).create(any());
    }
  }

  @Nested
  @DisplayName("Overlap Expiration")
  class OverlapExpiration {

    @Test
    @DisplayName("Should return correct overlap expiration time")
    void shouldReturnCorrectOverlapExpiration() {
      final var factory = mock(DataSourceFactory.class);
      final var oldDataSource = mock(DataSource.class);
      final var newDataSource = mock(DataSource.class);

      when(factory.create(any())).thenReturn(oldDataSource).thenReturn(newDataSource);

      final var overlapDuration = Duration.ofMinutes(15);
      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(factory)
              .overlapDuration(overlapDuration)
              .build();

      final var beforeReset = Instant.now();
      rotatingDs.reset();
      final var afterReset = Instant.now();

      final var expiresAt = rotatingDs.getOverlapExpiresAt().orElseThrow();

      assertTrue(expiresAt.isAfter(beforeReset.plus(overlapDuration)));
      assertTrue(expiresAt.isBefore(afterReset.plus(overlapDuration).plusSeconds(1)));
    }

    @Test
    @DisplayName("Should return empty when no overlap active")
    void shouldReturnEmptyWhenNoOverlapActive() {
      final var rotatingDs =
          RotatingDataSource.builder()
              .secretId("test-secret")
              .factory(mockFactory)
              .overlapDuration(Duration.ZERO)
              .build();

      assertTrue(rotatingDs.getOverlapExpiresAt().isEmpty());
    }
  }
}
