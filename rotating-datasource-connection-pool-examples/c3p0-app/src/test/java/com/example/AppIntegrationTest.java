package com.example;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.example.rotating.datasource.core.jdbc.DataSourceFactoryProvider;
import com.example.rotating.datasource.core.secrets.DbSecret;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.DriverManager;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/**
 * Integration test that exercises the App end-to-end and verifies it survives a database password
 * rotation while reusing a single RotatingDataSource instance across calls.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(named = "tests.integration.disable", matches = "true")
public class AppIntegrationTest {

  private static final String SECRET_ID = "db/secret";
  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3");
  private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:17");

  private PostgreSQLContainer<?> postgres;
  private GenericContainer<?> localstack;
  private SecretsManagerClient secretsClient;

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
  }

  @AfterAll
  void cleanup() {
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
   * Integration test for App:
   *
   * <ul>
   *   <li>The first call should succeed.
   *   <li>Rotates DB password and updates the secret.
   *   <li>Uses the current password from Secrets Manager to alter the user.
   *   <li>The second call must reuse the same RotatingDataSource instance and succeed after
   *       rotation.
   * </ul>
   */
  @Test
  void survivesRotationWithSingleRotatingDataSource() throws Exception {
    final var app = new App(SECRET_ID, buildFactory());

    final var first = app.getString();
    assertNotNull(first);
    assertFalse(first.isBlank());

    final var newPass = "rotated456";
    final var currentJson = secretsClient.getSecretValue(r -> r.secretId(SECRET_ID)).secretString();
    final var currentSecret = new ObjectMapper().readValue(currentJson, DbSecret.class);
    final var currentPassword = currentSecret.password();

    final var jdbcUrl =
        "jdbc:postgresql://%s:%d/%s"
            .formatted(
                postgres.getHost(), postgres.getFirstMappedPort(), postgres.getDatabaseName());
    try (final var conn =
        DriverManager.getConnection(jdbcUrl, postgres.getUsername(), currentPassword)) {
      conn.createStatement()
          .execute(
              """
              ALTER USER "%s" WITH PASSWORD '%s'
              """
                  .formatted(postgres.getUsername(), newPass));
    }

    final var rotatedJson = buildSecretJson(newPass);
    secretsClient.putSecretValue(r -> r.secretId(SECRET_ID).secretString(rotatedJson));

    final var second = app.getString();
    assertNotNull(second);
    assertFalse(second.isBlank());
  }

  private DataSourceFactoryProvider buildFactory() {
    return Pool.c3p0Factory;
  }

  private String buildSecretJson(String password) {
    return TestSupport.buildDbSecretJson(postgres, password);
  }
}
