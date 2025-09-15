package com.example.smrotator.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
public class RotatingDataSourceTest {

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

  @Test
  void shouldCreateAndUseInitialDataSource() throws Exception {
    final var rotating = new RotatingDataSource(SECRET_ID, hikariFactory());

    try (final var conn = rotating.getDataSource().getConnection();
        final var st = conn.createStatement();
        final var rs = st.executeQuery("SELECT 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }

    rotating.shutdown();
  }

  @Test
  void shouldResetAndSwapPoolClosingOldOne() throws Exception {
    final var rotating = new RotatingDataSource(SECRET_ID, hikariFactory());

    final var ds1 = (HikariDataSource) rotating.getDataSource();
    assertFalse(ds1.isClosed());

    rotating.reset();

    // After reset, we should have a different instance and the old should eventually be closed
    final var ds2 = (HikariDataSource) rotating.getDataSource();
    assertNotSame(ds1, ds2);

    // Wait up to 2 seconds for async close
    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!ds1.isClosed() && System.nanoTime() < deadline) {
      Thread.sleep(25);
    }
    assertTrue(ds1.isClosed(), "Old data source should be closed after reset");

    // New one works
    try (final var conn = ds2.getConnection();
        final var st = conn.createStatement();
        final var rs = st.executeQuery("SELECT 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }

    rotating.shutdown();
  }

  @Test
  void shouldShutdownCloseCurrentPool() throws Exception {
    final var rotating = new RotatingDataSource(SECRET_ID, hikariFactory());
    final var ds = (HikariDataSource) rotating.getDataSource();
    assertFalse(ds.isClosed());

    rotating.shutdown();

    // Give it a brief moment to close
    Thread.sleep(50);
    assertTrue(ds.isClosed());
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
      cfg.setPoolName("rotating-ds-it");
      // Be responsive in tests
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
    var endpoint = "http://%s:%d".formatted(localstack.getHost(), localstack.getMappedPort(4566));
    return SecretsManagerClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .build();
  }
}
