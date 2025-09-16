package com.example.rotatingdatasource.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URI;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(named = "tests.integration.disable", matches = "true")
public class SecretsManagerProviderTest {

  private static final String SECRET_ID = "test/secret";
  private static final String SECRET_VALUE =
      """
      {
        "username": "testuser",
        "password": "testpass",
        "engine": "postgres",
        "host": "localhost",
        "port": 5432,
        "dbname": "testdb"
      }
      """;

  private GenericContainer<?> localstack;
  private SecretsManagerClient directClient;

  @BeforeAll
  void setup() {
    assumeTrue(dockerAvailable(), "Docker not available, skipping test");

    localstack =
        new GenericContainer<>(DockerImageName.parse("localstack/localstack:3"))
            .withExposedPorts(4566)
            .withEnv("SERVICES", "secretsmanager");
    localstack.start();

    configureSecretsManagerForLocalstack();
    directClient = buildLocalstackClient();

    // Create test secret
    directClient.createSecret(r -> r.name(SECRET_ID).secretString(SECRET_VALUE));

    final var createdSecret =
        directClient.getSecretValue(r -> r.secretId(SECRET_ID)).secretString();
    assumeTrue(
        createdSecret.contains("testuser"),
        "Test secret was not created properly in Localstack. Got: %s".formatted(createdSecret));
  }

  @BeforeEach
  void resetSecretToOriginalValue() {
    directClient.putSecretValue(r -> r.secretId(SECRET_ID).secretString(SECRET_VALUE));
    SecretsManagerProvider.resetClient();
    System.clearProperty("aws.sm.cache.ttl.millis");
    // Ensure cache is disabled by default between tests
    SecretsManagerProvider.configureCacheForTests(0, null);
  }

  @AfterAll
  void cleanup() {
    if (directClient != null) {
      try {
        directClient.close();
      } catch (final Exception ignored) {
      }
    }
    if (localstack != null) localstack.stop();

    // Clean up system properties
    System.clearProperty("aws.sm.endpoint");
    System.clearProperty("aws.region");
    System.clearProperty("aws.accessKeyId");
    System.clearProperty("aws.secretAccessKey");
    System.clearProperty("aws.sm.cache.ttl.millis");
    SecretsManagerProvider.resetClient();
  }

  @Test
  void shouldGetSecretValue() {
    final var result = SecretsManagerProvider.getSecret(SECRET_ID);
    assertTrue(
        result.contains("testuser"),
        "Secret should contain testuser, but was: %s".formatted(result));
    assertTrue(
        result.contains("testpass"),
        "Secret should contain testpass, but was: %s".formatted(result));
  }

  @Test
  void shouldGetSecretVersion() {
    final var version = SecretsManagerProvider.getSecretVersion(SECRET_ID);
    assertNotNull(version, "Version should not be null");
    assertFalse(version.isBlank(), "Version should not be blank");
  }

  @Test
  void shouldGetDifferentVersionAfterUpdate() {
    final var originalVersion = SecretsManagerProvider.getSecretVersion(SECRET_ID);

    // Update the secret
    final var updatedValue = SECRET_VALUE.replace("testpass", "newpass");
    directClient.putSecretValue(r -> r.secretId(SECRET_ID).secretString(updatedValue));

    SecretsManagerProvider.resetClient();

    final var newVersion = SecretsManagerProvider.getSecretVersion(SECRET_ID);
    final var newValue = SecretsManagerProvider.getSecret(SECRET_ID);

    assertNotEquals(originalVersion, newVersion, "Version should change after update");
    assertTrue(newValue.contains("newpass"), "Updated secret should contain newpass");
    assertFalse(newValue.contains("testpass"), "Updated secret should not contain old password");
  }

  @Test
  void shouldReturnCachedValueWithinTtlAfterUpdate() throws Exception {
    // Enable cache with 1s TTL for this test
    SecretsManagerProvider.configureCacheForTests(1000, null);

    final var initialValue = SecretsManagerProvider.getSecret(SECRET_ID);
    final var initialVersion = SecretsManagerProvider.getSecretVersion(SECRET_ID);
    assertTrue(initialValue.contains("testpass"));

    // Update secret directly in Localstack
    final var updatedValue = SECRET_VALUE.replace("testpass", "newpass");
    directClient.putSecretValue(r -> r.secretId(SECRET_ID).secretString(updatedValue));

    // Immediately read again via provider: should still be cached old value and version
    final var cachedValue = SecretsManagerProvider.getSecret(SECRET_ID);
    final var cachedVersion = SecretsManagerProvider.getSecretVersion(SECRET_ID);
    assertEquals(initialVersion, cachedVersion, "Version should be served from cache within TTL");
    assertTrue(
        cachedValue.contains("testpass"),
        "Cached value should still contain old password within TTL");
    assertFalse(
        cachedValue.contains("newpass"),
        "Cached value should not yet contain new password within TTL");
  }

  @Test
  void shouldRefreshAfterTtlExpiry() throws Exception {
    // Enable cache with 200ms TTL for this test
    SecretsManagerProvider.configureCacheForTests(200, null);

    final var initialVersion = SecretsManagerProvider.getSecretVersion(SECRET_ID);

    // Update secret directly in Localstack
    final var updatedValue = SECRET_VALUE.replace("testpass", "newpass2");
    directClient.putSecretValue(r -> r.secretId(SECRET_ID).secretString(updatedValue));

    // Wait for TTL to expire
    Thread.sleep(300);

    final var newVersion = SecretsManagerProvider.getSecretVersion(SECRET_ID);
    final var newValue = SecretsManagerProvider.getSecret(SECRET_ID);

    assertNotEquals(initialVersion, newVersion, "Version should refresh after TTL expiry");
    assertTrue(newValue.contains("newpass2"), "Value should refresh after TTL expiry");
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
