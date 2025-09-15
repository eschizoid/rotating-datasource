package com.example.smrotator.core;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

/**
 * Provides a lazily configured AWS Secrets Manager client and convenience access to secret values.
 *
 * <p>Configuration can be supplied via system properties or environment variables:
 *
 * <ul>
 *   <li>aws.region / AWS_REGION
 *   <li>aws.sm.endpoint / AWS_SM_ENDPOINT (useful for Localstack)
 *   <li>aws.accessKeyId / AWS_ACCESS_KEY_ID
 *   <li>aws.secretAccessKey / AWS_SECRET_ACCESS_KEY
 *   <li>aws.sm.cache.ttl.millis / AWS_SM_CACHE_TTL_MILLIS (optional, default 0 = disabled)
 * </ul>
 */
public class SecretsManagerProvider {

  private static volatile SecretsManagerClient client;
  private static final ConcurrentHashMap<String, CacheEntry> CACHE = new ConcurrentHashMap<>();
  private static volatile long ttlMillis = initTtlMillis();
  private static volatile Clock clock = Clock.systemUTC();

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    final var c = client;
                    if (c != null) c.close();
                  } catch (final Exception ignored) {
                  }
                }));
  }

  private static long initTtlMillis() {
    final var sys = System.getProperty("aws.sm.cache.ttl.millis");
    final var env = System.getenv("AWS_SM_CACHE_TTL_MILLIS");
    final var val = sys != null ? sys : env;
    if (val == null || val.isBlank()) return 0L;
    try {
      final var parsed = Long.parseLong(val.trim());
      return Math.max(0L, parsed);
    } catch (final NumberFormatException e) {
      return 0L; // fail safe: disable cache on bad value
    }
  }

  /** For tests only: override TTL and clock. */
  static synchronized void configureCacheForTests(final long newTtlMillis, final Clock newClock) {
    ttlMillis = Math.max(0L, newTtlMillis);
    clock = newClock == null ? Clock.systemUTC() : newClock;
    CACHE.clear();
  }

  /** Clears the in-memory cache. */
  public static void resetCache() {
    CACHE.clear();
  }

  /** Also clears cache; next access will lazily rebuild client with current config. */
  public static synchronized void resetClient() {
    if (client != null) {
      try {
        client.close();
      } catch (final Exception ignored) {
      }
    }
    client = null;
    resetCache();
  }

  /**
   * Builds the {@link SecretsManagerClient} honoring region, endpoint and credentials overrides.
   *
   * @return configured {@link SecretsManagerClient}
   */
  private static SecretsManagerClient buildClient() {
    final var builder = SecretsManagerClient.builder();

    // Region from system property or env, default to us-east-1
    final var region =
        Optional.ofNullable(System.getProperty("aws.region"))
            .or(() -> Optional.ofNullable(System.getenv("AWS_REGION")))
            .map(Region::of)
            .orElse(Region.US_EAST_1);
    builder.region(region);

    // Endpoint override (useful for Localstack in tests)
    Optional.ofNullable(System.getProperty("aws.sm.endpoint"))
        .or(() -> Optional.ofNullable(System.getenv("AWS_SM_ENDPOINT")))
        .map(URI::create)
        .ifPresent(builder::endpointOverride);

    // Credentials: use system properties if provided, else default provider chain
    final var accessKey = System.getProperty("aws.accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    final var secretKey =
        System.getProperty("aws.secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    if (accessKey != null && secretKey != null)
      builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
    else builder.credentialsProvider(DefaultCredentialsProvider.builder().build());
    return builder.build();
  }

  /** Lazily gets the SecretsManagerClient, building it if necessary. */
  private static synchronized SecretsManagerClient getClient() {
    if (client == null) client = buildClient();
    return client;
  }

  /**
   * Retrieves the raw secret string for the given secret identifier. Uses cache when enabled (ttl >
   * 0).
   *
   * @param secretId the Secrets Manager secret ID or name
   * @return the secret string as stored in Secrets Manager
   */
  public static String getSecret(final String secretId) {
    if (ttlMillis > 0) {
      final var now = Instant.now(clock).toEpochMilli();
      final var cached = CACHE.get(secretId);
      if (cached != null && cached.expiresAtMillis >= now && cached.secretString != null) {
        return cached.secretString;
      }
      // Fetch and populate
      final var request = GetSecretValueRequest.builder().secretId(secretId).build();
      final var response = getClient().getSecretValue(request);
      final var entry =
          new CacheEntry(response.secretString(), response.versionId(), now + ttlMillis);
      CACHE.put(secretId, entry);
      return entry.secretString;
    } else {
      final var request = GetSecretValueRequest.builder().secretId(secretId).build();
      final var response = getClient().getSecretValue(request);
      return response.secretString();
    }
  }

  /**
   * Retrieves the current version identifier for the given secret. Uses cache when enabled (ttl >
   * 0).
   *
   * @param secretId the Secrets Manager secret ID or name
   * @return versionId of the latest secret value
   */
  public static String getSecretVersion(final String secretId) {
    if (ttlMillis > 0) {
      final var now = Instant.now(clock).toEpochMilli();
      final var cached = CACHE.get(secretId);
      if (cached != null && cached.expiresAtMillis >= now && cached.versionId != null) {
        return cached.versionId;
      }
      final var request = GetSecretValueRequest.builder().secretId(secretId).build();
      final var response = getClient().getSecretValue(request);
      final var entry =
          new CacheEntry(response.secretString(), response.versionId(), now + ttlMillis);
      CACHE.put(secretId, entry);
      return entry.versionId;
    } else {
      final var request = GetSecretValueRequest.builder().secretId(secretId).build();
      final var response = getClient().getSecretValue(request);
      return response.versionId();
    }
  }

  private record CacheEntry(String secretString, String versionId, long expiresAtMillis) {}
}
