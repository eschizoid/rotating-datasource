package com.example.rotatingdatasource.core.reactive;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.example.rotatingdatasource.core.secrets.SecretsManagerProvider;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import java.net.URI;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(named = "tests.integration.disable", matches = "true")
class RotatingConnectionFactoryIT {

  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3");
  private static final String SECRET_ID = "r2dbc/test/secret";

  private GenericContainer<?> localstack;
  private SecretsManagerClient secretsClient;
  private PostgreSQLContainer<?> postgres;

  private final AtomicReference<String> currentPassword = new AtomicReference<>("initialPass");

  @BeforeAll
  void setup() {
    assumeTrue(dockerAvailable(), "Docker not available, skipping integration test");

    // Start PostgreSQL with initial password
    postgres =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"))
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword(currentPassword.get());
    postgres.start();

    // Start Localstack for Secrets Manager
    localstack =
        new GenericContainer<>(LOCALSTACK_IMAGE)
            .withExposedPorts(4566)
            .withEnv("SERVICES", "secretsmanager")
            .withEnv("DEBUG", "1");
    localstack.start();

    configureSecretsManagerForLocalstack(localstack);
    secretsClient = buildLocalstackClient(localstack);

    final var initialSecretJson = buildSecretJson(postgres, currentPassword.get());
    secretsClient.createSecret(r -> r.name(SECRET_ID).secretString(initialSecretJson));
  }

  @AfterAll
  void cleanup() {
    if (secretsClient != null)
      try {
        secretsClient.close();
      } catch (Exception ignored) {
      }
    if (postgres != null) postgres.stop();
    if (localstack != null) localstack.stop();
  }

  @Test
  @DisplayName("Should create initial ConnectionFactory and perform a simple query")
  void shouldCreateAndQuery() {
    final var rcf =
        RotatingConnectionFactory.builder().secretId(SECRET_ID).factory(realProvider()).build();

    final var val = queryNow(rcf);
    assertNotNull(val);
    assertFalse(val.isBlank());

    rcf.shutdown().blockOptional();
  }

  @Test
  @DisplayName("Should proactively refresh after secret version change")
  void shouldProactivelyRefreshAfterSecretVersionChanges() throws Exception {
    final var rcf =
        RotatingConnectionFactory.builder()
            .secretId(SECRET_ID)
            .factory(realProvider())
            .refreshIntervalSeconds(1)
            .build();

    // Initial query succeeds
    final var first = queryNow(rcf);
    assertNotNull(first);
    assertFalse(first.isBlank());

    // Rotate DB password using current credentials
    final var newPass = "rotatedPass1";
    try (final var conn =
        DriverManager.getConnection(
            "jdbc:postgresql://%s:%d/%s"
                .formatted(
                    postgres.getHost(), postgres.getFirstMappedPort(), postgres.getDatabaseName()),
            postgres.getUsername(),
            currentPassword.get())) {
      conn.createStatement()
          .execute(
              "ALTER USER \"%s\" WITH PASSWORD '%s'".formatted(postgres.getUsername(), newPass));
    }

    // Update secret in Localstack
    currentPassword.set(newPass);
    secretsClient.putSecretValue(
        r -> r.secretId(SECRET_ID).secretString(buildSecretJson(postgres, newPass)));

    // Await up to ~10s for auto-refresh
    final var deadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
    String result = null;
    while (System.currentTimeMillis() < deadline) {
      try {
        result = queryNow(rcf);
        if (result != null && !result.isBlank()) break;
      } catch (Throwable ignored) {
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException ignored) {
      }
    }
    assertNotNull(result, "Factory did not auto-refresh within timeout");
    assertFalse(result.isBlank());

    rcf.shutdown().blockOptional();
  }

  @Test
  @DisplayName("Should reset and continue serving with new factory")
  void shouldResetAndSwapFactory() throws Exception {
    final var rcf =
        RotatingConnectionFactory.builder().secretId(SECRET_ID).factory(realProvider()).build();

    // Initial query succeeds
    final var first = queryNow(rcf);
    assertNotNull(first);
    assertFalse(first.isBlank());

    // Change password in DB and update secret
    final var newPass = "rotatedPass2";
    try (final var conn =
        DriverManager.getConnection(
            "jdbc:postgresql://%s:%d/%s"
                .formatted(
                    postgres.getHost(), postgres.getFirstMappedPort(), postgres.getDatabaseName()),
            postgres.getUsername(),
            currentPassword.get())) {
      conn.createStatement()
          .execute(
              "ALTER USER \"%s\" WITH PASSWORD '%s'".formatted(postgres.getUsername(), newPass));
    }
    currentPassword.set(newPass);
    secretsClient.putSecretValue(
        r -> r.secretId(SECRET_ID).secretString(buildSecretJson(postgres, newPass)));

    rcf.reset().block();

    final var second = queryNow(rcf);
    assertNotNull(second);
    assertFalse(second.isBlank());

    rcf.shutdown().blockOptional();
  }

  private ConnectionFactoryProvider realProvider() {
    return secret -> {
      final var cfg =
          PostgresqlConnectionConfiguration.builder()
              .host(secret.host())
              .port(secret.port())
              .database(secret.dbname())
              .username(secret.username())
              .password(secret.password())
              .build();
      final var base = new PostgresqlConnectionFactory(cfg);
      final var poolCfg = ConnectionPoolConfiguration.builder(base).build();
      return new ConnectionPool(poolCfg);
    };
  }

  private static boolean dockerAvailable() {
    try {
      // simple TCP check to local docker daemon via env; testcontainers does this internally
      Class.forName("org.testcontainers.DockerClientFactory");
      org.testcontainers.DockerClientFactory.instance().client();
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  private static void configureSecretsManagerForLocalstack(final GenericContainer<?> ls) {
    System.setProperty(
        "aws.sm.endpoint", "http://%s:%d".formatted(ls.getHost(), ls.getMappedPort(4566)));
    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.accessKeyId", "test");
    System.setProperty("aws.secretAccessKey", "test");
    SecretsManagerProvider.resetClient();
  }

  private static SecretsManagerClient buildLocalstackClient(final GenericContainer<?> ls) {
    final var endpoint = "http://%s:%d".formatted(ls.getHost(), ls.getMappedPort(4566));
    return SecretsManagerClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .build();
  }

  private static String buildSecretJson(final PostgreSQLContainer<?> pg, final String password) {
    return ("""
        {
          "username": "%s",
          "password": "%s",
          "engine": "postgres",
          "host": "%s",
          "port": %d,
          "dbname": "%s"
        }
        """)
        .formatted(
            pg.getUsername(),
            password,
            pg.getHost(),
            pg.getFirstMappedPort(),
            pg.getDatabaseName());
  }

  private static String queryNow(final ConnectionFactory cf) {
    return Mono.usingWhen(
            Mono.from(cf.create()),
            conn ->
                Mono.from(conn.createStatement("SELECT NOW()").execute())
                    .flatMap(
                        result -> Mono.from(result.map((row, md) -> row.get(0, String.class)))),
            conn -> Mono.from(conn.close()))
        .block();
  }
}
