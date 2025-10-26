package com.example.rotating.datasource.core.reactive;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.example.rotating.datasource.core.secrets.DbSecret;
import com.example.rotating.datasource.core.secrets.SecretHelper;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RotatingConnectionFactoryTest {

  private MockedStatic<SecretHelper> secretMock;
  private Connection dummyConn;

  @BeforeEach
  void setUp() {
    secretMock = mockStatic(SecretHelper.class);
    dummyConn = mock(Connection.class);

    // Default stubs (can be overridden per-test with thenReturn(...))
    secretMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v1");
    secretMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(new DbSecret("u", "v1", "postgres", "host", 5432, "db"));
  }

  @AfterEach
  void tearDown() {
    if (secretMock != null) secretMock.close();
    // Clear interrupt flag if any sleep was interrupted in prior tests
    if (Thread.currentThread().isInterrupted()) Thread.interrupted();
  }

  private ConnectionFactoryProvider providerWithBehavior() {
    // Build ConnectionFactory based on secret.password() marker
    return secret -> new TestCF("cf-" + secret.password());
  }

  @Test
  @DisplayName("scheduler checkAndRefresh switches primary and updates metadata")
  void schedulerCheckAndRefreshUpdatesPrimary() throws Exception {
    // v1 at build, then scheduler sees v2 and subsequent calls stay at v2
    secretMock
        .when(() -> SecretHelper.getSecretVersion(anyString()))
        .thenReturn("v1")
        .thenAnswer(inv -> "v2");
    secretMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(new DbSecret("u", "v1", "postgres", "host", 5432, "db"))
        .thenAnswer(inv -> new DbSecret("u", "v2", "postgres", "host", 5432, "db"));

    final var rcf =
        RotatingConnectionFactory.builder()
            .secretId("sid")
            .factory(providerWithBehavior())
            .refreshIntervalSeconds(0)
            .build();

    // initial metadata
    assertEquals("cf-v1", rcf.getMetadata().getName());

    // Invoke private checkAndRefresh() reflectively to avoid scheduler thread issues
    final var a = RotatingConnectionFactory.class.getDeclaredMethod("checkAndRefresh");
    a.setAccessible(true);
    a.invoke(rcf);

    // after refresh, metadata should reflect v2
    assertEquals("cf-v2", rcf.getMetadata().getName());

    rcf.shutdown().blockOptional();
  }

  @Test
  @DisplayName("reset() with overlap sets and clears overlap, exposing expiresAt")
  void resetOverlapSetsAndClears() throws Exception {
    // Build with two sequential secrets so reset swaps to v2 and keeps v1 secondary
    secretMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v1", "v2");
    secretMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(
            new DbSecret("u", "v1", "postgres", "host", 5432, "db"),
            new DbSecret("u", "v2", "postgres", "host", 5432, "db"));

    final var rcf =
        RotatingConnectionFactory.builder()
            .secretId("sid")
            .factory(providerWithBehavior())
            .overlapDuration(Duration.ofMillis(200))
            .gracePeriod(Duration.ofMillis(50))
            .build();

    // Trigger reset to create secondary and set overlap window
    rcf.reset().block();

    assertTrue(rcf.isOverlapActive(), "overlap should be active immediately after reset");
    assertTrue(rcf.getOverlapExpiresAt().isPresent(), "expiresAt should be present");

    // Wait for overlap to elapse and disposal lambda to run
    Thread.sleep(400);

    assertFalse(rcf.isOverlapActive(), "overlap should be cleared after duration");
    assertTrue(rcf.getOverlapExpiresAt().isEmpty(), "expiresAt should be empty after end");

    rcf.shutdown().blockOptional();
  }

  @Test
  @DisplayName("create(): auth error triggers reset then succeeds on new factory")
  void createAuthErrorRetriesOnceThenSucceeds() {
    // v1: auth fails; v2: success
    secretMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v1", "v1", "v2");
    secretMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(
            new DbSecret("u", "v1-authfail", "postgres", "host", 5432, "db"),
            new DbSecret("u", "v2", "postgres", "host", 5432, "db"));

    ConnectionFactoryProvider provider =
        secret -> {
          final String pw = secret.password();
          if (pw.contains("authfail")) return new TestCF("cf-auth").authFailAlways();
          return new TestCF("cf-ok");
        };

    final var rcf = RotatingConnectionFactory.builder().secretId("sid").factory(provider).build();

    // Attempt should perform reset on auth error and then succeed
    assertNotNull(Mono.from(rcf.create()).block());

    rcf.shutdown().blockOptional();
  }

  @Test
  @DisplayName("create(): transient errors retry until success")
  void createTransientRetryUntilSuccess() {
    // single version, factory will transient-fail twice then succeed
    secretMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v1");
    secretMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(new DbSecret("u", "v1-transient", "postgres", "host", 5432, "db"));

    ConnectionFactoryProvider provider = secret -> new TestCF("cf-t").transientFailNTimes(2);

    final var rcf = RotatingConnectionFactory.builder().secretId("sid").factory(provider).build();

    assertNotNull(Mono.from(rcf.create()).block());

    rcf.shutdown().blockOptional();
  }

  @Test
  @DisplayName("version check failure is swallowed (onErrorResume) and connection still acquired")
  void versionCheckFailureIsSwallowed() {
    // Initial build uses v1; later, the checkLatestVersion throws
    secretMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v1");
    secretMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(new DbSecret("u", "v1", "postgres", "host", 5432, "db"));

    final var rcf =
        RotatingConnectionFactory.builder().secretId("sid").factory(providerWithBehavior()).build();

    // Now make the version check throw during create()
    secretMock
        .when(() -> SecretHelper.getSecretVersion(anyString()))
        .thenThrow(new RuntimeException("boom"));

    assertNotNull(Mono.from(rcf.create()).block());

    rcf.shutdown().blockOptional();
  }

  @Test
  @DisplayName("tryGetConnectionWithFallback: during overlap, primary auth fails -> secondary used")
  void fallbackToSecondaryDuringOverlapOnAuthFailure() throws Exception {
    // Build v1, then reset to v2 so v1 becomes secondary; make v2 primary auth-fail
    secretMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v1", "v2");
    secretMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(
            new DbSecret("u", "v1", "postgres", "host", 5432, "db"),
            new DbSecret("u", "v2-authfail", "postgres", "host", 5432, "db"));

    ConnectionFactoryProvider provider =
        secret -> {
          final String pw = secret.password();
          if (pw.contains("authfail")) return new TestCF("cf-primary").authFailAlways();
          return new TestCF("cf-secondary");
        };

    final var rcf =
        RotatingConnectionFactory.builder()
            .secretId("sid")
            .factory(provider)
            .overlapDuration(Duration.ofSeconds(1))
            .build();

    // reset swaps to v2 primary and keeps v1 as secondary within overlap window
    rcf.reset().block();
    assertTrue(rcf.isOverlapActive());

    // create(): primary will auth-fail, fallback to secondary should succeed
    assertNotNull(Mono.from(rcf.create()).block());

    rcf.shutdown().blockOptional();
  }

  @Test
  @DisplayName("reset() with zero overlap triggers grace-period disposal path (no exception)")
  void resetZeroOverlapGraceDisposalPath() throws Exception {
    secretMock.when(() -> SecretHelper.getSecretVersion(anyString())).thenReturn("v1", "v2");
    secretMock
        .when(() -> SecretHelper.getDbSecret(anyString()))
        .thenReturn(
            new DbSecret("u", "v1", "postgres", "host", 5432, "db"),
            new DbSecret("u", "v2", "postgres", "host", 5432, "db"));

    final var rcf =
        RotatingConnectionFactory.builder()
            .secretId("sid")
            .factory(providerWithBehavior())
            .overlapDuration(Duration.ZERO)
            .gracePeriod(Duration.ofMillis(100))
            .build();

    rcf.reset().block();

    // Wait enough for grace period disposal lambda to run; no observable effect, just ensure no
    // crash
    TimeUnit.MILLISECONDS.sleep(200);
    assertNotNull(rcf.getMetadata());

    rcf.shutdown().blockOptional();
  }

  private final class TestCF implements ConnectionFactory {
    private final String name;
    private final AtomicInteger failures = new AtomicInteger(0);
    private volatile Throwable failAlways;
    private volatile int transientFailTimes;

    TestCF(String name) {
      this.name = name;
    }

    TestCF authFailAlways() {
      this.failAlways = new R2dbcNonTransientResourceException("auth", "28000", 0);
      return this;
    }

    TestCF transientFailNTimes(int times) {
      this.transientFailTimes = times;
      return this;
    }

    @Override
    public Publisher<? extends Connection> create() {
      // Always-auth-fail branch
      if (failAlways != null) {
        return Mono.<Connection>error(failAlways);
      }
      // Transient failures first, then succeed
      if (failures.getAndIncrement() < transientFailTimes) {
        return Mono.<Connection>error(new R2dbcTransientResourceException("temp", "08006", 0));
      }
      return Mono.just(dummyConn);
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
      final String n = name;
      return () -> n;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
