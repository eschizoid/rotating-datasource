package com.example;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.example.rotatingdatasource.core.SecretsManagerProvider;
import java.net.URI;
import java.time.Instant;
import java.util.Objects;
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
public class JooqMainIntegrationTest {
  private static final String SECRET_ID = "it/orm/jooq/secret";

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

    try {
      Main.shutdownRotating();
    } catch (Throwable ignored) {
    }

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

    try {
      final var dsl = Main.buildDsl(rotating);

      // No explicit retry needed - RotatingDataSource handles it internally
      final var result = Objects.requireNonNull(dsl.fetchOne("select now()")).get(0, Instant.class);

      assertNotNull(result);
    } finally {
      Main.shutdownRotating();
    }
  }

  /**
   * Verifies that RotatingDataSource's built-in retry handles authentication errors transparently
   * when credentials are rotated. The pool automatically detects auth failures and refreshes.
   */
  @Test
  void shouldHandleAuthFailureWithRepositoryAfterBadCredentials() throws Exception {
    final var rotating = Main.rotatingDataSource(SECRET_ID);

    try {
      final var dsl = Main.buildDsl(rotating);

      // Sanity check first query works
      final var before = Objects.requireNonNull(dsl.fetchOne("select now()")).get(0, Instant.class);
      assertNotNull(before);

      // Change password in database first
      final var newPassword = "rotated_password";
      postgres.execInContainer(
          "psql",
          "-U",
          postgres.getUsername(),
          "-c",
          "ALTER USER " + postgres.getUsername() + " WITH PASSWORD '" + newPassword + "';");

      // Update secret with new password BEFORE triggering retry
      smClient.updateSecret(r -> r.secretId(SECRET_ID).secretString(pgSecretJson(newPassword)));
      SecretsManagerProvider.resetCache();

      // This query should transparently retry - RotatingDataSource.getConnection()
      // detects auth error and automatically calls reset()
      final var after = dsl.fetchOne("select now()").get(0, Instant.class);
      assertNotNull(after);

    } finally {
      Main.shutdownRotating();
    }
  }

  private String pgSecretJson() {
    return pgSecretJson(postgres.getPassword());
  }

  private String pgSecretJson(String password) {
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
    var endpoint = "http://%s:%d".formatted(localstack.getHost(), localstack.getMappedPort(4566));
    return SecretsManagerClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .build();
  }
}
