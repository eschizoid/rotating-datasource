package com.example;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.example.smrotator.core.DataSourceFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/**
 * Load test to ensure the Tomcat JDBC pool is not exhausted under concurrent queries and there are
 * no leaks.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(named = "tests.integration.disable", matches = "true")
public class ConnectionLoadTest {

  private static final String SECRET_ID = "db/secret-load-tomcat";
  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3");
  private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:17");

  private PostgreSQLContainer<?> postgres;
  private GenericContainer<?> localstack;
  private SecretsManagerClient secretsClient;
  private App app;

  @BeforeAll
  void setup() {
    assumeTrue(TestSupport.dockerAvailable(), "Docker not available, skipping integration test");

    postgres =
        new PostgreSQLContainer<>(POSTGRES_IMAGE)
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");
    postgres.start();

    localstack =
        new GenericContainer<>(LOCALSTACK_IMAGE)
            .withExposedPorts(4566)
            .withEnv("SERVICES", "secretsmanager")
            .withEnv("DEBUG", "1");
    localstack.start();

    TestSupport.configureSecretsManagerForLocalstack(localstack);
    secretsClient = TestSupport.buildLocalstackClient(localstack);

    final var initialSecretJson = TestSupport.buildDbSecretJson(postgres, postgres.getPassword());
    secretsClient.createSecret(r -> r.name(SECRET_ID).secretString(initialSecretJson));

    app = new App(SECRET_ID, buildFactory());
  }

  @AfterAll
  void cleanup() {
    if (app != null) app.shutdown();
    if (secretsClient != null)
      try {
        secretsClient.close();
      } catch (Exception ignored) {
      }
    if (postgres != null) postgres.stop();
    if (localstack != null) localstack.stop();
  }

  @Test
  void highConcurrencyDoesNotExhaustConnections() throws Exception {
    final var threads = 24; // > max pool size (10)
    final var iterationsPerThread = 100;
    final var testTimeout = Duration.ofSeconds(60);

    final var pool = Executors.newFixedThreadPool(threads);
    final var futures = new ArrayList<Future<Boolean>>();

    for (var t = 0; t < threads; t++) {
      futures.add(
          pool.submit(
              () -> {
                for (var i = 0; i < iterationsPerThread; i++) {
                  final var s = app.getString();
                  if (s == null || s.isBlank()) return false;
                }
                return true;
              }));
    }
    pool.shutdown();
    final var finished = pool.awaitTermination(testTimeout.toMillis(), TimeUnit.MILLISECONDS);
    assertTrue(finished, "Load test did not finish within timeout; potential deadlock/exhaustion");

    for (final var f : futures) {
      assertTrue(f.get(5, TimeUnit.SECONDS), "Worker failed during load");
    }
  }

  private DataSourceFactory buildFactory() {
    return Pool.tomcatFactory;
  }
}
