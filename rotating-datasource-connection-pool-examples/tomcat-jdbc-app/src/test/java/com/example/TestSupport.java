package com.example;

import com.example.rotating.datasource.core.secrets.SecretsManagerProvider;
import java.net.URI;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/** Test-only utilities for integration tests to reduce duplication. */
public final class TestSupport {
  private TestSupport() {}

  /** Simple Docker availability probe using Testcontainers. */
  public static boolean dockerAvailable() {
    try {
      DockerClientFactory.instance().client();
      return true;
    } catch (final Throwable t) {
      return false;
    }
  }

  /**
   * Configures the production SecretsManagerProvider to point at the given Localstack container and
   * resets the singleton client so changes take effect for the running JVM.
   */
  public static void configureSecretsManagerForLocalstack(final GenericContainer<?> localstack) {
    System.setProperty(
        "aws.sm.endpoint",
        "http://%s:%d".formatted(localstack.getHost(), localstack.getMappedPort(4566)));
    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.accessKeyId", "test");
    System.setProperty("aws.secretAccessKey", "test");
    SecretsManagerProvider.resetClient();
  }

  /** Builds a SecretsManagerClient pointed at the Localstack container. */
  public static SecretsManagerClient buildLocalstackClient(final GenericContainer<?> localstack) {
    final var endpoint =
        "http://%s:%d".formatted(localstack.getHost(), localstack.getMappedPort(4566));
    return SecretsManagerClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .build();
  }

  /** Builds a JSON string matching DbSecret from a Postgres container and password. */
  public static String buildDbSecretJson(
      final PostgreSQLContainer<?> postgres, final String password) {
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
            postgres.getUsername(),
            password,
            postgres.getHost(),
            postgres.getFirstMappedPort(),
            postgres.getDatabaseName());
  }
}
