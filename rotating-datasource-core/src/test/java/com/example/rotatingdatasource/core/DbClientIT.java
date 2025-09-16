package com.example.rotatingdatasource.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
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
public class DbClientIT {

  private PostgreSQLContainer<?> postgres;
  private GenericContainer<?> localstack;
  private SecretsManagerClient smClient;
  private RotatingDataSource rotatingDs;

  private static final String SECRET_ID = "it/pg/dbclient";

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

    rotatingDs = RotatingDataSource.builder().secretId(SECRET_ID).factory(hikariFactory()).build();
  }

  @AfterEach
  void resetBetweenTests() {
    SecretsManagerProvider.resetClient();
  }

  @AfterAll
  void cleanup() {
    if (rotatingDs != null) rotatingDs.shutdown();

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

  @Test
  void executeWithRetryShouldExecuteQuerySuccessfully() throws SQLException {
    final var client = new DbClient(rotatingDs);

    final var result =
        client.executeWithRetry(
            conn -> {
              try (final var st = conn.createStatement();
                  final var rs = st.executeQuery("SELECT 42")) {
                assertTrue(rs.next());
                return rs.getInt(1);
              }
            },
            Retry.Policy.fixed(2, 1_000));

    assertEquals(42, result);
  }

  @Test
  @Disabled
  void executeWithRetryShouldRecoverAfterPasswordRotation() throws Exception {
    final var client = new DbClient(rotatingDs);

    // Initial successful query
    final var before =
        client.executeWithRetry(
            conn -> {
              try (final var st = conn.createStatement();
                  final var rs = st.executeQuery("SELECT 1")) {
                rs.next();
                return rs.getInt(1);
              }
            },
            Retry.Policy.fixed(2, 1_000));
    assertEquals(1, before);

    // Simulate password rotation by updating secret
    final var newPassword = "rotated_password";
    postgres.execInContainer(
        "psql",
        "-U",
        postgres.getUsername(),
        "-c",
        "ALTER USER %s WITH PASSWORD '%s';".formatted(postgres.getUsername(), newPassword));

    smClient.updateSecret(
        r -> r.secretId(SECRET_ID).secretString(pgSecretJson(postgres, newPassword)));

    // Force pool to pick up new credentials
    rotatingDs.reset();
    Thread.sleep(100); // Allow async close to complete

    // Should work with new credentials
    final var after =
        client.executeWithRetry(
            conn -> {
              try (final var st = conn.createStatement();
                  final var rs = st.executeQuery("SELECT 2")) {
                rs.next();
                return rs.getInt(1);
              }
            },
            Retry.Policy.fixed(2, 1_000));
    assertEquals(2, after);
  }

  @Test
  void executeWithRetryShouldRetryOnAuthFailure() throws SQLException {
    final var attemptCounter = new AtomicInteger(0);
    final var client = new DbClient(rotatingDs);

    final var result =
        client.executeWithRetry(
            conn -> {
              if (attemptCounter.incrementAndGet() == 1) {
                throw new SQLException("password authentication failed for user", "28P01");
              }
              return "success";
            },
            Retry.Policy.fixed(2, 1_000));

    assertEquals("success", result);
    assertEquals(2, attemptCounter.get());
  }

  private DataSourceFactory hikariFactory() {
    return secret -> {
      final var cfg = new HikariConfig();
      final var url =
          "jdbc:postgresql://%s:%d/%s".formatted(secret.host(), secret.port(), secret.dbname());
      cfg.setJdbcUrl(url);
      cfg.setUsername(secret.username());
      cfg.setPassword(secret.password());
      cfg.setMaximumPoolSize(2);
      cfg.setPoolName("dbclient-it");
      cfg.setConnectionTimeout(Duration.ofSeconds(2).toMillis());
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
