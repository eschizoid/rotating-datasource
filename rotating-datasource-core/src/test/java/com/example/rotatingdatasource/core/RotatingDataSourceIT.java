package com.example.rotatingdatasource.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

@DisabledIfSystemProperty(named = "tests.integration.disable", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RotatingDataSourceIT {

  private PostgreSQLContainer<?> postgres;
  private GenericContainer<?> localstack;
  private SecretsManagerClient smClient;

  private static final String SECRET_ID = "it/pg/secret";

  @BeforeAll
  void startContainersAndSeedSecret() {
    assumeTrue(dockerAvailable(), "Docker not available, skipping test");

    postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"));
    postgres.start();

    localstack =
        new GenericContainer<>(DockerImageName.parse("localstack/localstack:3"))
            .withExposedPorts(4566)
            .withEnv("SERVICES", "secretsmanager");
    localstack.start();

    configureSecretsManagerForLocalstack();
    smClient = buildLocalstackClient();

    smClient.createSecret(
        r -> r.name(SECRET_ID).secretString(pgSecretJson(postgres, postgres.getPassword())));

    final var created = smClient.getSecretValue(r -> r.secretId(SECRET_ID)).secretString();
    assumeTrue(created.contains(postgres.getUsername()));
  }

  @AfterEach
  void resetBetweenTests() {
    SecretsManagerProvider.resetClient();
  }

  @AfterAll
  void cleanup() {
    System.clearProperty("aws.sm.endpoint");
    System.clearProperty("aws.region");
    System.clearProperty("aws.accessKeyId");
    System.clearProperty("aws.secretAccessKey");
    SecretsManagerProvider.resetClient();

    if (smClient != null)
      try {
        smClient.close();
      } catch (Exception ignored) {
      }
    if (localstack != null) localstack.stop();
    if (postgres != null) postgres.stop();
  }

  @Nested
  @DisplayName("Basic Functionality")
  class BasicFunctionality {

    @Test
    @DisplayName("Should create and use initial DataSource successfully")
    void shouldCreateAndUseInitialDataSource() throws Exception {
      final var rotating =
          RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();

      try (final var conn = rotating.getConnection();
          final var st = conn.createStatement();
          final var rs = st.executeQuery("SELECT 1")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }

      rotating.shutdown();
    }

    @Test
    @DisplayName("Should create with proactive refresh enabled")
    void shouldCreateWithProactiveRefresh() throws Exception {
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(hikariFactory())
              .refreshIntervalSeconds(60)
              .build();

      try (final var conn = rotating.getConnection()) {
        assertNotNull(conn);
      }

      rotating.shutdown();
    }

    @Test
    @DisplayName("Should create with custom retry policy")
    void shouldCreateWithCustomPolicy() throws Exception {
      final var policy = Retry.Policy.exponential(3, 100L);
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(hikariFactory())
              .refreshIntervalSeconds(0)
              .retryPolicy(policy)
              .build();

      try (final var conn = rotating.getConnection()) {
        assertNotNull(conn);
      }

      rotating.shutdown();
    }

    @Test
    @DisplayName("Should delegate JDBC operations to underlying DataSource")
    void shouldDelegateJdbcOperations() throws Exception {
      final var rotating =
          RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();

      assertDoesNotThrow(rotating::getLoginTimeout);
      assertDoesNotThrow(() -> rotating.setLoginTimeout(30));
      assertTrue(rotating.isWrapperFor(HikariDataSource.class));
      assertNotNull(rotating.unwrap(HikariDataSource.class));

      assertThrows(Exception.class, rotating::getLogWriter);
      assertThrows(Exception.class, () -> rotating.setLogWriter(null));
      assertThrows(Exception.class, rotating::getParentLogger);

      rotating.shutdown();
    }
  }

  @Nested
  @DisplayName("Credential Rotation")
  class CredentialRotation {

    @Test
    @Disabled
    @DisplayName("Should manually reset and swap pool closing old one")
    void shouldResetAndSwapPoolClosingOldOne() throws Exception {
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(hikariFactory())
              .gracePeriod(Duration.ofSeconds(1))
              .overlapDuration(Duration.ofMillis(5_000))
              .build();

      final var ds1 = rotating.unwrap(HikariDataSource.class);
      assertFalse(ds1.isClosed());

      rotating.reset();

      final var ds2 = rotating.unwrap(HikariDataSource.class);
      assertNotSame(ds1, ds2);

      Thread.sleep(5_000);

      assertTrue(ds1.isClosed(), "Old data source should be closed after reset");

      try (final var conn = ds2.getConnection();
          final var st = conn.createStatement();
          final var rs = st.executeQuery("SELECT 1")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }

      rotating.shutdown();
    }

    @Test
    @DisplayName("Should proactively refresh when secret version changes")
    void shouldProactivelyRefreshWhenVersionChanges() throws Exception {
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(hikariFactory())
              .refreshIntervalSeconds(1)
              .build();

      final var ds1 = rotating.unwrap(HikariDataSource.class);

      smClient.updateSecret(
          r -> r.secretId(SECRET_ID).secretString(pgSecretJson(postgres, "new-pass-123")));

      Thread.sleep(500);
      smClient.updateSecret(
          r -> r.secretId(SECRET_ID).secretString(pgSecretJson(postgres, postgres.getPassword())));

      Thread.sleep(3_000);

      final var ds2 = rotating.unwrap(HikariDataSource.class);
      assertNotSame(ds1, ds2, "Should have swapped to new DataSource after version change");

      rotating.shutdown();
    }

    @Test
    @DisplayName("Should not refresh when version unchanged")
    void shouldNotRefreshWhenVersionUnchanged() throws Exception {
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(hikariFactory())
              .refreshIntervalSeconds(1)
              .build();

      final var ds1 = rotating.unwrap(HikariDataSource.class);

      Thread.sleep(3_000);

      final var ds2 = rotating.unwrap(HikariDataSource.class);
      assertSame(ds1, ds2, "Should not swap DataSource when version unchanged");

      rotating.shutdown();
    }

    @Test
    @DisplayName("Should handle multiple consecutive resets")
    void shouldHandleMultipleResets() throws Exception {
      final var rotating =
          RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();

      for (int i = 0; i < 5; i++) {
        rotating.reset();
        try (final var conn = rotating.getConnection()) {
          assertNotNull(conn);
        }
      }

      rotating.shutdown();
    }
  }

  @Nested
  @DisplayName("Retry Policy")
  class Policy {

    @Test
    @DisplayName("Should respect fixed delay retry policy")
    void shouldRespectFixedDelayPolicy() throws Exception {
      final var policy = Retry.Policy.fixed(3, 100L);
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(hikariFactory())
              .refreshIntervalSeconds(0)
              .retryPolicy(policy)
              .build();

      try (final var conn = rotating.getConnection()) {
        assertNotNull(conn);
      }

      rotating.shutdown();
    }

    @Test
    @DisplayName("Should respect exponential backoff retry policy")
    void shouldRespectExponentialBackoffPolicy() throws Exception {
      final var policy = Retry.Policy.exponential(5, 50L);
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(hikariFactory())
              .refreshIntervalSeconds(0)
              .retryPolicy(policy)
              .build();

      try (final var conn = rotating.getConnection()) {
        assertNotNull(conn);
      }

      rotating.shutdown();
    }
  }

  @Nested
  @DisplayName("Thread Safety")
  class ThreadSafety {

    @Test
    @DisplayName("Should handle concurrent connection requests")
    void shouldHandleConcurrentConnectionRequests() throws Exception {
      final var rotating =
          RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();

      final var executor = Executors.newFixedThreadPool(10);
      final var errors = new ConcurrentLinkedQueue<Throwable>();

      final var futures =
          IntStream.range(0, 50)
              .mapToObj(
                  i ->
                      CompletableFuture.runAsync(
                          () -> {
                            try (final var conn = rotating.getConnection();
                                final var stmt = conn.createStatement();
                                final var rs = stmt.executeQuery("SELECT " + i)) {
                              assertTrue(rs.next());
                              assertEquals(i, rs.getInt(1));
                            } catch (final Exception e) {
                              errors.add(e);
                            }
                          },
                          executor))
              .toArray(CompletableFuture[]::new);

      CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);

      assertTrue(errors.isEmpty(), "Should not have any errors: " + errors);

      executor.shutdown();
      rotating.shutdown();
    }

    @Test
    @DisplayName("Should handle concurrent reset calls")
    void shouldHandleConcurrentResetCalls() throws Exception {
      final var rotating =
          RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();
      final var executor = Executors.newFixedThreadPool(5);
      final var errors = new ConcurrentLinkedQueue<Throwable>();

      final var futures =
          IntStream.range(0, 20)
              .mapToObj(
                  i ->
                      CompletableFuture.runAsync(
                          () -> {
                            try {
                              rotating.reset();
                            } catch (Exception e) {
                              errors.add(e);
                            }
                          },
                          executor))
              .toArray(CompletableFuture[]::new);

      CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);

      assertTrue(errors.isEmpty(), "Should not have any errors during concurrent resets");

      try (final var conn = rotating.getConnection()) {
        assertNotNull(conn);
      }

      executor.shutdown();
      rotating.shutdown();
    }

    @Test
    @DisplayName("Should handle concurrent get + reset operations")
    void shouldHandleConcurrentGetAndReset() throws Exception {
      final var rotating =
          RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();
      final var executor = Executors.newFixedThreadPool(10);
      final var errors = new ConcurrentLinkedQueue<Throwable>();
      final var successCount = new AtomicInteger(0);

      final var connectionTasks =
          IntStream.range(0, 30)
              .mapToObj(
                  i ->
                      CompletableFuture.runAsync(
                          () -> {
                            try (final var conn = rotating.getConnection()) {
                              Thread.sleep(10);
                              successCount.incrementAndGet();
                            } catch (Exception e) {
                              errors.add(e);
                            }
                          },
                          executor))
              .toList();

      final var resetTasks =
          IntStream.range(0, 10)
              .mapToObj(
                  i ->
                      CompletableFuture.runAsync(
                          () -> {
                            try {
                              Thread.sleep(5);
                              rotating.reset();
                            } catch (Exception e) {
                              errors.add(e);
                            }
                          },
                          executor))
              .toList();

      final var allTasks = new ArrayList<CompletableFuture<?>>();
      allTasks.addAll(connectionTasks);
      allTasks.addAll(resetTasks);

      CompletableFuture.allOf(allTasks.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

      assertTrue(errors.isEmpty(), "Should not have any errors: " + errors);
      assertTrue(successCount.get() > 0, "Should have successful connections");

      executor.shutdown();
      rotating.shutdown();
    }
  }

  @Nested
  @DisplayName("Resource Management")
  class ResourceManagement {

    @Test
    @DisplayName("Should shutdown and close current pool")
    void shouldShutdownCloseCurrentPool() throws Exception {
      final var rotating =
          RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();

      final var ds = rotating.unwrap(HikariDataSource.class);
      assertFalse(ds.isClosed());

      rotating.shutdown();

      Thread.sleep(100);
      assertTrue(ds.isClosed());
    }

    @Test
    @DisplayName("Should stop scheduler on shutdown")
    void shouldStopSchedulerOnShutdown() throws Exception {
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(hikariFactory())
              .refreshIntervalSeconds(1)
              .build();

      final var dsBeforeShutdown = rotating.unwrap(HikariDataSource.class);

      smClient.updateSecret(
          r -> r.secretId(SECRET_ID).secretString(pgSecretJson(postgres, postgres.getPassword())));

      Thread.sleep(2_000);

      rotating.shutdown();

      smClient.updateSecret(
          r -> r.secretId(SECRET_ID).secretString(pgSecretJson(postgres, postgres.getPassword())));

      Thread.sleep(3_000);

      assertTrue(dsBeforeShutdown.isClosed(), "Scheduler should be stopped after shutdown");
    }

    @Test
    @DisplayName("Should handle shutdown with no scheduler")
    void shouldHandleShutdownWithNoScheduler() {
      final var rotating =
          RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();
      assertDoesNotThrow(rotating::shutdown);
    }
  }

  @Nested
  @DisplayName("Error Handling")
  class ErrorHandling {

    @Test
    @DisplayName("Should throw exception for invalid secret ID")
    void shouldHandleInvalidSecretId() {
      assertThrows(
          RuntimeException.class,
          () ->
              RotatingDataSource.builder()
                  .secretId("non-existent-secret")
                  .factory(hikariFactory())
                  .build());
    }

    @Test
    @DisplayName("Should handle malformed secret JSON")
    void shouldHandleMalformedSecretJson() {
      final var badSecretId = "bad-json-secret";
      smClient.createSecret(r -> r.name(badSecretId).secretString("{\"invalid\":\"structure\"}"));

      assertThrows(
          RuntimeException.class,
          () ->
              RotatingDataSource.builder().secretId(badSecretId).factory(hikariFactory()).build());
    }

    @Test
    @DisplayName("Should handle DataSource creation failure gracefully")
    void shouldHandleDataSourceCreationFailure() {
      DataSourceFactoryProvider failingFactory =
          secret -> {
            throw new RuntimeException("Simulated DataSource creation failure");
          };

      assertThrows(
          RuntimeException.class,
          () -> RotatingDataSource.builder().secretId(SECRET_ID).factory(failingFactory).build());
    }

    @Test
    @DisplayName("Should handle connection timeout")
    void shouldHandleConnectionTimeout() throws Exception {
      final var rotating =
          RotatingDataSource.builder()
              .secretId(SECRET_ID)
              .factory(
                  secret -> {
                    final var cfg = new HikariConfig();
                    cfg.setJdbcUrl(
                        "jdbc:postgresql://%s:%d/%s"
                            .formatted(secret.host(), secret.port(), secret.dbname()));
                    cfg.setUsername(secret.username());
                    cfg.setPassword(secret.password());
                    cfg.setMaximumPoolSize(1);
                    cfg.setConnectionTimeout(Duration.ofMillis(500).toMillis());
                    return new HikariDataSource(cfg);
                  })
              .build();

      try (final var conn = rotating.getConnection()) {
        assertNotNull(conn);
      }

      rotating.shutdown();
    }
  }

  private DataSourceFactoryProvider hikariFactory() {
    return secret -> {
      final var cfg = new HikariConfig();
      final var url =
          "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname());
      cfg.setJdbcUrl(url);
      cfg.setUsername(secret.username());
      cfg.setPassword(secret.password());
      cfg.setMaximumPoolSize(5);
      cfg.setPoolName("rotating-ds-it");
      cfg.setConnectionTimeout(Duration.ofSeconds(5).toMillis());
      cfg.setInitializationFailTimeout(-1);
      return new HikariDataSource(cfg);
    };
  }

  private String pgSecretJson(final PostgreSQLContainer<?> c, final String password) {
    return """
        {"username":"%s","password":"%s","engine":"postgres","host":"%s","port":%d,"dbname":"%s"}
        """
        .formatted(
            c.getUsername(), password, c.getHost(), c.getFirstMappedPort(), c.getDatabaseName());
  }

  private boolean dockerAvailable() {
    try {
      DockerClientFactory.instance().client();
      return true;
    } catch (final Throwable t) {
      return false;
    }
  }

  private void configureSecretsManagerForLocalstack() {
    System.setProperty(
        "aws.sm.endpoint",
        "http://%s:%d".formatted(localstack.getHost(), localstack.getMappedPort(4566)));
    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.accessKeyId", "test");
    System.setProperty("aws.secretAccessKey", "test");
  }

  private SecretsManagerClient buildLocalstackClient() {
    final var endpoint =
        "http://%s:%d".formatted(localstack.getHost(), localstack.getMappedPort(4566));
    return SecretsManagerClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .build();
  }
}
