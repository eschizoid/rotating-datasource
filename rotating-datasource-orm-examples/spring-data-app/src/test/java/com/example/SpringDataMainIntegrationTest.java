package com.example;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.example.rotatingdatasource.core.SecretsManagerProvider;
import java.net.URI;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
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
public class SpringDataMainIntegrationTest {
  private static final String SECRET_ID = "it/orm/spring-data/secret";

  private PostgreSQLContainer<?> postgres;
  private GenericContainer<?> localstack;
  private SecretsManagerClient smClient;
  private ConfigurableApplicationContext ctx;

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
    // Disable Boot logging system to avoid SLF4J binding conflicts in test runtime
    System.setProperty("org.springframework.boot.logging.LoggingSystem", "none");

    ctx = SpringApplication.run(Main.class);
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

    if (ctx != null) ctx.close();

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
    final var repo = ctx.getBean(TestRepository.class);

    final var result = repo.now();

    assertNotNull(result);
  }

  @Test
  void shouldRetryAuthErrorTransparentlyWithRepository() throws Exception {
    final var repo = ctx.getBean(TestRepository.class);

    // Initial query works
    final var before = repo.now();
    assertNotNull(before);

    // Rotate password in the database
    final var newPassword = "rotated_password";
    postgres.execInContainer(
        "psql",
        "-U",
        postgres.getUsername(),
        "-c",
        "ALTER USER " + postgres.getUsername() + " WITH PASSWORD '" + newPassword + "';");

    // Update secret with new password
    smClient.updateSecret(r -> r.secretId(SECRET_ID).secretString(pgSecretJson(newPassword)));
    SecretsManagerProvider.resetCache();

    // Should automatically retry and succeed through Spring Data JPA
    final var after = repo.now();
    assertNotNull(after);
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
