package com.example.smrotator.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Supplier;

/**
 * Utility helper for retrieving and parsing secrets from AWS Secrets Manager.
 *
 * <p>This class provides convenience methods to fetch the secret string, obtain its version, and
 * deserialize it into a {@link DbSecret} using Jackson.
 */
public class SecretHelper {

  private static Supplier<ObjectMapper> mapperSupplier = ObjectMapper::new;

  /**
   * Sets the supplier of the {@link ObjectMapper} to use for deserialization.
   *
   * @param supplier the supplier of the {@link ObjectMapper} to use
   */
  public static void setMapperSupplier(final Supplier<ObjectMapper> supplier) {
    mapperSupplier = supplier;
  }

  /**
   * Retrieves a DB secret from AWS Secrets Manager and converts it into a {@link DbSecret}.
   *
   * @param secretId the identifier/name of the secret in AWS Secrets Manager
   * @return the parsed {@link DbSecret}
   * @throws RuntimeException if the secret cannot be fetched or parsed
   */
  public static DbSecret getDbSecret(final String secretId) {
    try {
      final var secret = SecretsManagerProvider.getSecret(secretId);
      return mapperSupplier.get().readValue(secret, DbSecret.class);
    } catch (final Exception exception) {
      throw new RuntimeException("Failed to load DB secret", exception);
    }
  }

  /**
   * Retrieves the current version identifier of a secret from AWS Secrets Manager.
   *
   * @param secretId the identifier/name of the secret
   * @return the version id string for the latest version of the secret
   * @throws RuntimeException if the version cannot be retrieved
   */
  public static String getSecretVersion(final String secretId) {
    try {
      return SecretsManagerProvider.getSecretVersion(secretId);
    } catch (final Exception exception) {
      throw new RuntimeException("Failed to retrieve secret version", exception);
    }
  }
}
