package com.example;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.util.concurrent.Executors.*;
import static java.util.concurrent.TimeUnit.*;
import static java.util.stream.Collectors.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.example.rotating.datasource.core.secrets.SecretsManagerProvider;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
public class HibernateMainIntegrationTest {

  private static final String SECRET_ID = "it/orm/hibernate/secret";
  private static final System.Logger LOGGER =
      System.getLogger(HibernateMainIntegrationTest.class.getName());

  private PostgreSQLContainer<?> postgres;
  private GenericContainer<?> localstack;
  private SecretsManagerClient smClient;

  @BeforeAll
  void setUp() {
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

    smClient.createSecret(r -> r.name(SECRET_ID).secretString(pgSecretJson()));

    final var created = smClient.getSecretValue(r -> r.secretId(SECRET_ID)).secretString();
    assumeTrue(created.contains(postgres.getUsername()));

    System.setProperty("db.secretId", SECRET_ID);
  }

  @AfterEach
  void resetBetweenTests() {
    SecretsManagerProvider.resetClient();
    SecretsManagerProvider.resetCache();
  }

  @AfterAll
  void tearDown() {
    System.clearProperty("aws.sm.endpoint");
    System.clearProperty("aws.region");
    System.clearProperty("aws.accessKeyId");
    System.clearProperty("aws.secretAccessKey");
    System.clearProperty("db.secretId");
    SecretsManagerProvider.resetClient();

    if (smClient != null)
      try {
        smClient.close();
      } catch (final Exception ignored) {
      }
    if (localstack != null) localstack.stop();
    if (postgres != null) postgres.stop();
  }

  @Test
  void mainWiringShouldExecuteSelectNow() {
    final var rotating = Main.rotatingDataSource(SECRET_ID);

    try (final var sf = Main.buildSessionFactory(rotating);
        final var session = sf.openSession()) {

      final var result = session.createNativeQuery("SELECT now()", Instant.class).getSingleResult();

      assertNotNull(result);
    } finally {
      Main.shutdownRotating();
    }
  }

  @Test
  void shouldHandleAuthFailureWithRepositoryAfterBadCredentials() throws Exception {
    final var rotating = Main.rotatingDataSource(SECRET_ID);

    try (final var sf = Main.buildSessionFactory(rotating);
        final var session = sf.openSession()) {

      final var before = session.createNativeQuery("SELECT now()", Instant.class).getSingleResult();
      assertNotNull(before);

      final var newPassword = "rotated_password";
      postgres.execInContainer(
          "psql",
          "-U",
          postgres.getUsername(),
          "-c",
          "ALTER USER %s WITH PASSWORD '%s';".formatted(postgres.getUsername(), newPassword));

      smClient.updateSecret(r -> r.secretId(SECRET_ID).secretString(pgSecretJson(newPassword)));
      SecretsManagerProvider.resetCache();

      final var after = session.createNativeQuery("SELECT now()", Instant.class).getSingleResult();
      assertNotNull(after);

    } finally {
      Main.shutdownRotating();
    }
  }

  @Test
  void shouldDrainInFlightConnectionsDuringConcurrentPoolSwap() throws Exception {
    final var rotating = Main.rotatingDataSource(SECRET_ID);

    try (final var sf = Main.buildSessionFactory(rotating)) {
      // Ensure table exists and seed a few rows
      try (final var session = sf.openSession()) {
        final var tx = session.beginTransaction();
        session
            .createNativeMutationQuery(
                "CREATE TABLE IF NOT EXISTS test_users (id SERIAL PRIMARY KEY, username VARCHAR(255), email VARCHAR(255), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            .executeUpdate();
        for (int i = 0; i < 10; i++) {
          session
              .createNativeMutationQuery(
                  "INSERT INTO test_users (username, email) VALUES (:username, :email) ON CONFLICT DO NOTHING")
              .setParameter("username", "u%d".formatted(i))
              .setParameter("email", "u%d@example.com".formatted(i))
              .executeUpdate();
        }
        tx.commit();
      }

      final int holders = 8;
      final int workers = 8;
      final var startLatch = new CountDownLatch(holders);
      final var finishLatch = new CountDownLatch(holders);
      final var successCount = new AtomicInteger(0);
      final var errors = new ConcurrentLinkedQueue<Throwable>();

      final var holderPool = newFixedThreadPool(holders);
      for (int i = 0; i < holders; i++) {
        holderPool.submit(
            () -> {
              try (final var session = sf.openSession()) {
                // Acquire a connection by executing a query, then keep the session alive across
                // rotation
                final var before =
                    session.createNativeQuery("SELECT now()", Instant.class).getSingleResult();
                assertNotNull(before);
                startLatch.countDown();
                Thread.sleep(1500); // keep the underlying connection alive during rotation
                final var after =
                    session.createNativeQuery("SELECT now()", Instant.class).getSingleResult();
                assertNotNull(after);
                successCount.incrementAndGet();
              } catch (final Throwable t) {
                errors.add(t);
              } finally {
                finishLatch.countDown();
              }
            });
      }

      assertTrue(startLatch.await(10, SECONDS), "Holders should all be in-flight");

      // Rotate password while holders have active transactions (pool swap)
      final var newPassword = "rotated_now%d".formatted(System.nanoTime());
      postgres.execInContainer(
          "psql",
          "-U",
          postgres.getUsername(),
          "-c",
          "ALTER USER %s WITH PASSWORD '%s';".formatted(postgres.getUsername(), newPassword));
      smClient.updateSecret(r -> r.secretId(SECRET_ID).secretString(pgSecretJson(newPassword)));
      SecretsManagerProvider.resetCache();

      // In parallel, keep performing operations to exercise new pool
      final var workerPool = newFixedThreadPool(workers);
      final var workerStop = new AtomicBoolean(false);
      final var workerErrors = new ConcurrentLinkedQueue<Throwable>();
      for (int i = 0; i < workers; i++) {
        final int w = i;
        workerPool.submit(
            () -> {
              try {
                while (!workerStop.get()) {
                  try (final var session = sf.openSession()) {
                    session.createNativeQuery("SELECT now()", Instant.class).getSingleResult();
                    final var tx = session.beginTransaction();
                    session
                        .createNativeMutationQuery(
                            "INSERT INTO test_users (username, email) VALUES (:u, :e)")
                        .setParameter("u", "w%d_%d".formatted(w, System.nanoTime()))
                        .setParameter("e", "w%d@example.com".formatted(w))
                        .executeUpdate();
                    tx.commit();
                  }
                  Thread.sleep(50);
                }
              } catch (final Throwable t) {
                workerErrors.add(t);
              }
            });
      }

      // Wait for in-flight holders to finish
      assertTrue(finishLatch.await(60, SECONDS), "All holders should finish and commit");
      workerStop.set(true);
      workerPool.shutdown();
      workerPool.awaitTermination(60, SECONDS);
      holderPool.shutdown();
      holderPool.awaitTermination(60, SECONDS);

      // Assertions: holders sessions survived rotation and continued to work
      assertTrue(
          successCount.get() >= (int) Math.floor(holders * 0.75),
          "Most in-flight sessions should keep working across rotation; successes=%d"
              .formatted(successCount.get()));

      if (!errors.isEmpty())
        errors.forEach(t -> LOGGER.log(ERROR, String.format("Holder error: %s%n", t)));

      if (!workerErrors.isEmpty())
        workerErrors.forEach(t -> LOGGER.log(ERROR, String.format("Worker error: %s%n", t)));

      // With internal retry logic in RotatingDataSource/Retry, no errors should leak to
      // holders/workers
      assertTrue(errors.isEmpty(), "No holder errors expected during swap: %s".formatted(errors));
      assertTrue(
          workerErrors.isEmpty(),
          "No worker errors expected during swap: %s".formatted(workerErrors));

    } finally {
      Main.shutdownRotating();
    }
  }

  @Test
  void shouldHandlePasswordRotationsUnderConcurrentLoadFor20seconds() throws Exception {
    final var rotating = Main.rotatingDataSource(SECRET_ID);

    try (final var sf = Main.buildSessionFactory(rotating)) {

      // Setup: Create test table and initial data
      try (final var session = sf.openSession()) {
        final var tx = session.beginTransaction();
        session
            .createNativeMutationQuery(
                "CREATE TABLE IF NOT EXISTS test_users (id SERIAL PRIMARY KEY, username VARCHAR(255), email VARCHAR(255), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            .executeUpdate();

        for (int i = 0; i < 100; i++) {
          session
              .createNativeMutationQuery(
                  "INSERT INTO test_users (username, email) VALUES (:username, :email)")
              .setParameter("username", "user%d".formatted(i))
              .setParameter("email", "user%d@example.com".formatted(i))
              .executeUpdate();
        }
        tx.commit();
      }

      final var testDuration = Duration.ofSeconds(20);
      final var startTime = Instant.now();
      final var stopFlag = new AtomicBoolean(false);

      final var workerThreads = 10;
      final var executor = newFixedThreadPool(workerThreads);
      final var errors = new ConcurrentLinkedQueue<Exception>();
      final var successfulOps = new AtomicInteger(0);
      final var rotationCount = new AtomicInteger(0);

      // Password rotation scheduler - rotate every 10 seconds
      final var rotationScheduler = newSingleThreadScheduledExecutor();
      rotationScheduler.scheduleAtFixedRate(
          () -> {
            if (stopFlag.get()) {
              return;
            }
            try {
              final var rotation = rotationCount.incrementAndGet();
              final var newPassword = "rotated_pass_%d".formatted(rotation);

              LOGGER.log(
                  INFO,
                  String.format(
                      "[%s] Password rotation %d: changing to '%s'%n",
                      Instant.now(), rotation, newPassword));

              // Change password in database
              postgres.execInContainer(
                  "psql",
                  "-U",
                  postgres.getUsername(),
                  "-c",
                  "ALTER USER %s WITH PASSWORD '%s';"
                      .formatted(postgres.getUsername(), newPassword));

              // Update secret in Secrets Manager
              smClient.updateSecret(
                  r -> r.secretId(SECRET_ID).secretString(pgSecretJson(newPassword)));
              SecretsManagerProvider.resetCache();

              LOGGER.log(
                  INFO,
                  String.format("[%s] Password rotation %d: completed%n", Instant.now(), rotation));

            } catch (final Exception exception) {
              LOGGER.log(
                  ERROR,
                  String.format(
                      "[%s] Password rotation failed: %s%n",
                      Instant.now(), exception.getMessage()));
              errors.add(exception);
            }
          },
          2_000, // Initial delay: 2 seconds
          10_000, // Rotate every 10 seconds
          MILLISECONDS);

      // Worker threads performing mixed workload
      final var workerLatch = new CountDownLatch(workerThreads);
      for (var i = 0; i < workerThreads; i++) {
        final int workerId = i;
        final var workloadType = i % 3; // 0=SELECT, 1=UPDATE, 2=INSERT/DELETE

        executor.submit(
            () -> {
              try {
                while (!stopFlag.get()) {
                  try {
                    switch (workloadType) {
                      case 0 -> { // SELECT operations
                        try (final var session = sf.openSession()) {
                          final var result =
                              session
                                  .createNativeQuery(
                                      "SELECT username, email FROM test_users WHERE id = :id",
                                      Object[].class)
                                  .setParameter("id", (workerId % 100) + 1)
                                  .getSingleResult();
                          if (result != null) successfulOps.incrementAndGet();
                        }
                      }
                      case 1 -> { // UPDATE operations
                        try (final var session = sf.openSession()) {

                          final var tx = session.beginTransaction();
                          final var updated =
                              session
                                  .createNativeMutationQuery(
                                      "UPDATE test_users SET email = :email, updated_at = CURRENT_TIMESTAMP WHERE id = :id")
                                  .setParameter(
                                      "email", "updated%d@example.com".formatted(workerId))
                                  .setParameter("id", (workerId % 100) + 1)
                                  .executeUpdate();
                          tx.commit();
                          if (updated > 0) successfulOps.incrementAndGet();
                        }
                      }
                      case 2 -> { // INSERT and DELETE operations
                        try (final var session = sf.openSession()) {
                          final var tx = session.beginTransaction();

                          // Insert
                          session
                              .createNativeMutationQuery(
                                  "INSERT INTO test_users (username, email) VALUES (:username, :email)")
                              .setParameter(
                                  "username", "temp_%d_%d".formatted(workerId, System.nanoTime()))
                              .setParameter("email", "temp%d@example.com".formatted(workerId))
                              .executeUpdate();

                          // Delete oldest temp user
                          session
                              .createNativeMutationQuery(
                                  "DELETE FROM test_users WHERE id IN (SELECT id FROM test_users WHERE username LIKE 'temp_%' ORDER BY id LIMIT 1)")
                              .executeUpdate();

                          tx.commit();
                          successfulOps.incrementAndGet();
                        }
                      }
                    }

                    Thread.sleep(50 + (int) (Math.random() * 100)); // Random delay 50-150ms
                  } catch (final Exception e) {
                    errors.add(e);
                  }
                }
              } finally {
                workerLatch.countDown();
              }
            });
      }

      // Wait for test duration
      Thread.sleep(testDuration.toMillis());
      stopFlag.set(true);

      // Shutdown and wait for completion
      rotationScheduler.shutdownNow();
      workerLatch.await(60, SECONDS);

      executor.shutdown();
      executor.awaitTermination(15, SECONDS);

      final var endTime = Instant.now();
      final var actualDuration = Duration.between(startTime, endTime);

      // Print results
      LOGGER.log(
          INFO,
          String.format(
              """

                                ==============================================
                                Test Results Summary:
                                ==============================================
                                Duration: %s seconds
                                Password Rotations: %d
                                Successful Operations: %d
                                Errors: %d
                                Operations per second: %.2f
                                ==============================================
                                """,
              actualDuration.getSeconds(),
              rotationCount.get(),
              successfulOps.get(),
              errors.size(),
              successfulOps.get() / (double) actualDuration.getSeconds()));

      if (!errors.isEmpty()) {
        LOGGER.log(ERROR, "Errors encountered:");
        errors.stream()
            .limit(10)
            .forEach(
                e ->
                    LOGGER.log(
                        ERROR,
                        String.format(
                            "  - %s: %s%n", e.getClass().getSimpleName(), e.getMessage())));
      }

      // Verify data integrity
      try (final var session = sf.openSession()) {
        final var count =
            session
                .createNativeQuery("SELECT COUNT(*) FROM test_users", Long.class)
                .getSingleResult()
                .intValue();
        assertTrue(count >= 100, "Should have at least initial 100 records");
      }

      // Assertions
      assertTrue(
          actualDuration.getSeconds() >= 15 && actualDuration.getSeconds() <= 40,
          "Test should run for ~20 seconds");
      assertTrue(rotationCount.get() >= 1, "Should perform at least one password rotation");
      assertTrue(
          successfulOps.get() > 50,
          "Should complete many operations successfully. Completed: %d"
              .formatted(successfulOps.get()));
      assertTrue(
          errors.isEmpty(),
          "No errors should occur during rotations with internal retry handling. Found: %s"
              .formatted(errors.stream().map(Throwable::getMessage).collect(joining(", "))));

    } finally {
      Main.shutdownRotating();
    }
  }

  private String pgSecretJson() {
    return pgSecretJson(postgres.getPassword());
  }

  private String pgSecretJson(final String password) {
    return """
        {"username":"%s","password":"%s","engine":"postgres","host":"%s","port":%d,"dbname":"%s"}
        """
        .formatted(
            postgres.getUsername(),
            password,
            postgres.getHost(),
            postgres.getFirstMappedPort(),
            postgres.getDatabaseName());
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
