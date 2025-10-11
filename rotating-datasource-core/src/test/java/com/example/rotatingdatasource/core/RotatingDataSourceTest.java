package com.example.rotatingdatasource.core;

import static com.example.rotatingdatasource.core.Retry.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class RotatingDataSourceTest {

  private MockedStatic<SecretHelper> secretHelperMock;
  private DataSource mockDataSource;
  private Connection mockConnection;

  @BeforeEach
  void setUp() {
    secretHelperMock = mockStatic(SecretHelper.class);
    mockDataSource = mock(DataSource.class);
    mockConnection = mock(Connection.class);

    secretHelperMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(new DbSecret("user", "pass", "testdb", "localhost", 5432, "testdb"));
    secretHelperMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v1");
  }

  @AfterEach
  void tearDown() {
    if (secretHelperMock != null) secretHelperMock.close();
  }

  @Test
  void testResetWithDataSourceCreationFailure() {
    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(secret -> mockDataSource)
            .build();

    secretHelperMock
        .when(() -> SecretHelper.getDbSecret("test-secret"))
        .thenThrow(new RuntimeException("Secret fetch failed"));

    assertThatThrownBy(rotatingDs::reset)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Secret fetch failed");
  }

  @Test
  void testGetConnectionWithUsernameAndPassword() throws SQLException {
    when(mockDataSource.getConnection("testuser", "testpass")).thenReturn(mockConnection);

    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(secret -> mockDataSource)
            .build();
    final var conn = rotatingDs.getConnection("testuser", "testpass");

    assertThat(conn).isNotNull();
    verify(mockDataSource).getConnection("testuser", "testpass");
  }

  @Test
  void testGetConnectionWithUsernamePasswordRetriesOnAuthError() throws SQLException {
    final var authError = new SQLException("Access denied", "28000");
    when(mockDataSource.getConnection("testuser", "testpass"))
        .thenThrow(authError)
        .thenReturn(mockConnection);

    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(secret -> mockDataSource)
            .build();
    final var conn = rotatingDs.getConnection("testuser", "testpass");

    assertThat(conn).isNotNull();
    verify(mockDataSource, times(2)).getConnection("testuser", "testpass");
  }

  @Test
  void testSetLogWriter() throws SQLException {
    final var writer = new PrintWriter(System.out);
    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(secret -> mockDataSource)
            .build();

    rotatingDs.setLogWriter(writer);

    verify(mockDataSource).setLogWriter(writer);
  }

  @Test
  void testGetLogWriter() throws SQLException {
    final var writer = new PrintWriter(System.out);
    when(mockDataSource.getLogWriter()).thenReturn(writer);

    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(secret -> mockDataSource)
            .build();
    final var result = rotatingDs.getLogWriter();

    assertThat(result).isEqualTo(writer);
    verify(mockDataSource).getLogWriter();
  }

  @Test
  void testGetParentLoggerThrowsException() throws SQLFeatureNotSupportedException {
    when(mockDataSource.getParentLogger()).thenThrow(new RuntimeException("Not supported"));

    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(secret -> mockDataSource)
            .build();

    assertThatThrownBy(rotatingDs::getParentLogger)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Not supported");
  }

  @Test
  void testShutdownWithAutoCloseableDataSourceThrowingException() throws Exception {
    final var closeableDs =
        mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
    final var autoCloseable = (AutoCloseable) closeableDs;
    doThrow(new RuntimeException("Close failed")).when(autoCloseable).close();

    final var rotatingDs =
        RotatingDataSource.builder().secretId("test-secret").factory(secret -> closeableDs).build();

    assertThatCode(rotatingDs::shutdown).doesNotThrowAnyException();
  }

  @Test
  void testResetClosesOldDataSourceAsynchronously() throws Exception {
    final var closeableDs =
        mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
    final var autoCloseable = (AutoCloseable) closeableDs;

    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(secret -> closeableDs)
            .refreshIntervalSeconds(0L)
            .retryPolicy(Retry.Policy.fixed(2, 50L))
            .authErrorDetector(Retry.AuthErrorDetector.defaultDetector())
            .overlapDuration(Duration.ZERO)
            .gracePeriod(Duration.ofMillis(100))
            .build();

    secretHelperMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v2");

    rotatingDs.reset();

    CompletableFuture.runAsync(
            () -> {
              try {
                TimeUnit.MILLISECONDS.sleep(200);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            })
        .join();

    verify(autoCloseable).close();
  }

  @Test
  void testCurrentSwapsDataSourceWhenVersionChanges() throws SQLException {
    final var ds1 = mock(DataSource.class);
    final var ds2 = mock(DataSource.class);
    final var conn1 = mock(Connection.class);
    final var conn2 = mock(Connection.class);

    when(ds1.getConnection()).thenReturn(conn1);
    when(ds2.getConnection()).thenReturn(conn2);

    final var callCount = new int[] {0};
    final DataSourceFactory factory =
        secret -> {
          callCount[0]++;
          return callCount[0] == 1 ? ds1 : ds2;
        };

    final var rotatingDs =
        RotatingDataSource.builder().secretId("test-secret").factory(factory).build();

    secretHelperMock.when(() -> SecretHelper.getSecretVersion("test-secret")).thenReturn("v1");
    final var firstConn = rotatingDs.getConnection();
    assertThat(firstConn).isSameAs(conn1);

    secretHelperMock.when(() -> SecretHelper.getSecretVersion("test-secret")).thenReturn("v2");
    final var secondConn = rotatingDs.getConnection();
    assertThat(secondConn).isNotNull();
    assertThat(secondConn).isSameAs(conn2);
  }

  @Test
  @Disabled("FIXME")
  void testOverlapDrainingAndFallbackBehavior() throws Exception {
    // Prepare two DataSources: ds1 (old), ds2 (new)
    final var ds1 = mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
    final var ds2 = mock(DataSource.class);
    final var conn1 = mock(Connection.class);
    final var conn2 = mock(Connection.class);

    when(ds1.getConnection()).thenReturn(conn1);
    when(ds2.getConnection()).thenReturn(conn2);

    // Factory returns ds1 first, then ds2 after rotation
    final var callCount = new int[] {0};
    final DataSourceFactory factory =
        secret -> {
          callCount[0]++;
          return callCount[0] == 1 ? ds1 : ds2;
        };

    // Use short overlap to make test fast
    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(factory)
            .refreshIntervalSeconds(0L)
            .retryPolicy(Retry.Policy.fixed(2, 10L))
            .authErrorDetector(AuthErrorDetector.defaultDetector())
            .overlapDuration(Duration.ofMillis(200))
            .build();

    // Initial state: version v1, primary = ds1
    secretHelperMock.when(() -> SecretHelper.getSecretVersion("test-secret")).thenReturn("v1");
    final var firstConn = rotatingDs.getConnection();
    assertThat(firstConn).isSameAs(conn1);

    // Trigger rotation to v2; primary becomes ds2, secondary becomes ds1
    secretHelperMock.when(() -> SecretHelper.getSecretVersion("test-secret")).thenReturn("v2");
    final var secondConn = rotatingDs.getConnection();
    assertThat(secondConn).isSameAs(conn2);

    // Verify that when primary succeeds, secondary is NOT used for new requests
    verify(ds1, times(1)).getConnection();
    verify(ds2, atLeastOnce()).getConnection();

    // Now simulate an auth error on primary and ensure fallback to secondary occurs
    final var authError = new SQLException("Access denied", "28000");
    when(ds2.getConnection())
        .thenThrow(authError)
        .thenReturn(conn2); // first call fails, then recovers

    final var fallbackConn = rotatingDs.getConnection();
    // On first attempt, tryGetConnectionWithFallback should use ds1 when ds2 fails with auth
    assertThat(fallbackConn).isSameAs(conn1);

    // Wait longer for overlap expiry + async cleanup to complete
    Thread.sleep(500);
    verify((AutoCloseable) ds1, atLeastOnce()).close();

    rotatingDs.shutdown();
  }

  @Test
  @Disabled("FIXME")
  void testOverlapDetectionApi() throws Exception {
    final var ds1 = mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
    final var ds2 = mock(DataSource.class);
    final var conn1 = mock(Connection.class);
    final var conn2 = mock(Connection.class);

    when(ds1.getConnection()).thenReturn(conn1);
    when(ds2.getConnection()).thenReturn(conn2);

    final var callCount = new int[] {0};
    final DataSourceFactory factory =
        secret -> {
          callCount[0]++;
          return callCount[0] == 1 ? ds1 : ds2;
        };

    final var rotatingDs =
        RotatingDataSource.builder()
            .secretId("test-secret")
            .factory(factory)
            .refreshIntervalSeconds(0L)
            .retryPolicy(Retry.Policy.fixed(2, 10L))
            .authErrorDetector(AuthErrorDetector.defaultDetector())
            .overlapDuration(Duration.ofMillis(150))
            .build();

    // Initial state
    secretHelperMock.when(() -> SecretHelper.getSecretVersion("test-secret")).thenReturn("v1");
    rotatingDs.getConnection();
    assertThat(rotatingDs.isOverlapActive()).isFalse();
    assertThat(rotatingDs.getOverlapExpiresAt()).isEmpty();

    // Rotate
    secretHelperMock.when(() -> SecretHelper.getSecretVersion("test-secret")).thenReturn("v2");
    rotatingDs.getConnection();
    assertThat(rotatingDs.isOverlapActive()).isTrue();
    assertThat(rotatingDs.getOverlapExpiresAt()).isPresent();

    // Wait longer for overlap expiry + async cleanup to complete
    Thread.sleep(400);
    assertThat(rotatingDs.isOverlapActive()).isFalse();
    assertThat(rotatingDs.getOverlapExpiresAt()).isEmpty();

    rotatingDs.shutdown();
  }
}
