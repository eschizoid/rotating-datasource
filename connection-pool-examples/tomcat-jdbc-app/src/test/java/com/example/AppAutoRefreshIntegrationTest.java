package com.example;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.example.smrotator.core.DataSourceFactory;
import com.example.smrotator.core.DbSecret;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/**
 * Integration test that exercises the App end-to-end and verifies it auto-refreshes after a secret
 * version change while reusing a single RotatingDataSource instance across calls.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(named = "tests.integration.disable", matches = "true")
public class AppAutoRefreshIntegrationTest {

  private static final String SECRET_ID = "db/secret-autorefresh";
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

    // Configure the production SecretsManagerProvider to point to Localstack and rebuild client
    TestSupport.configureSecretsManagerForLocalstack(localstack);

    secretsClient = TestSupport.buildLocalstackClient(localstack);

    // Create a secret that matches DbSecret structure and points to our PostgreSQLContainer
    final var initialSecretJson = buildSecretJson(postgres.getPassword());
    secretsClient.createSecret(r -> r.name(SECRET_ID).secretString(initialSecretJson));

    // Build an App that uses a RotatingDataSource with proactive refresh every 1 second
    app = new App(SECRET_ID, buildFactory(), 1L);
  }

  @AfterAll
  void cleanup() {
    if (app != null) {
      app.shutdown();
    }
    if (secretsClient != null) {
      try {
        secretsClient.close();
      } catch (final Exception ignored) {
      }
    }
    if (postgres != null) postgres.stop();
    if (localstack != null) localstack.stop();
  }

  /**
   * Integration test for App with proactive auto-refresh enabled:
   *
   * <ul>
   *   <li>The first call should succeed.
   *   <li>Rotates DB password and updates the secret, producing a new version.
   *   <li>Waits for the scheduler to detect the change and refresh the pool (no DbClient reactive
   *       retry).
   *   <li>Later App calls succeed using the refreshed pool.
   * </ul>
   */
  @Test
  void autoRefreshesAfterSecretVersionChange() throws Exception {
    // Initial call should succeed using the initial credentials
    final var first = app.getString();
    assertNotNull(first);
    assertFalse(first.isBlank());

    // Rotate: compute a new password, fetch the current password from Secrets Manager for ALTER
    // USER
    final var newPass = "autoRotate789";
    final var currentJson = secretsClient.getSecretValue(r -> r.secretId(SECRET_ID)).secretString();
    final var currentSecret = new ObjectMapper().readValue(currentJson, DbSecret.class);

    // Change DB password using the currently valid password
    final var jdbcUrl =
        "jdbc:postgresql://%s:%d/%s"
            .formatted(
                postgres.getHost(), postgres.getFirstMappedPort(), postgres.getDatabaseName());
    try (final var conn =
        DriverManager.getConnection(jdbcUrl, postgres.getUsername(), currentSecret.password())) {
      conn.createStatement()
          .execute(
              """
              ALTER USER "%s" WITH PASSWORD '%s'
              """
                  .formatted(postgres.getUsername(), newPass));
    }

    // Update the secret in Secrets Manager (new version)
    final var rotatedJson = buildSecretJson(newPass);
    secretsClient.putSecretValue(r -> r.secretId(SECRET_ID).secretString(rotatedJson));

    // Await up to ~10 seconds for the scheduler (1s interval) to detect the change and refresh
    final var deadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
    final var result = retryUntilSuccess(deadline);
    assertNotNull(result);
    assertFalse(result.isBlank());
  }

  private String retryUntilSuccess(long deadline) throws SQLException, InterruptedException {
    while (System.currentTimeMillis() < deadline) {
      try {
        final var val = app.getString();
        if (val != null && !val.isBlank()) {
          return val;
        }
      } catch (final SQLException exception) {
        if (System.currentTimeMillis() >= deadline) {
          throw new SQLException(
              "Call failed after waiting for auto-refresh: %s".formatted(exception.getMessage()),
              exception);
        }
        Thread.sleep(300);
      }
    }
    throw new SQLException("Timeout waiting for auto-refresh");
  }

  private DataSourceFactory buildFactory() {
    return Pool.tomcatFactory;
  }

  private String buildSecretJson(final String password) {
    return TestSupport.buildDbSecretJson(postgres, password);
  }
}
