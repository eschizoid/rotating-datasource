package com.example.rotating.datasource.core.secrets;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

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

  private static final ConcurrentHashMap<String, CacheEntry> CACHE = new ConcurrentHashMap<>();
  private static volatile SecretsManagerClient client;
  private static volatile long ttlMillis = initTtlMillis();
  private static volatile Clock clock = Clock.systemUTC();

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () ->
                    Optional.ofNullable(client)
                        .ifPresent(
                            c -> {
                              try {
                                c.close();
                              } catch (final Exception ignored) {
                              }
                            })));
  }

  private static long initTtlMillis() {
    return Optional.ofNullable(System.getProperty("aws.sm.cache.ttl.millis"))
        .or(() -> Optional.ofNullable(System.getenv("AWS_SM_CACHE_TTL_MILLIS")))
        .filter(val -> !val.isBlank())
        .map(String::trim)
        .flatMap(
            val -> {
              try {
                return Optional.of(Long.parseLong(val));
              } catch (final NumberFormatException e) {
                return Optional.empty();
              }
            })
        .map(parsed -> Math.max(0L, parsed))
        .orElse(0L);
  }

  /** For tests only: override TTL and clock. */
  public static synchronized void configureCacheForTests(
      final long newTtlMillis, final Clock newClock) {
    ttlMillis = Math.max(0L, newTtlMillis);
    clock = Optional.ofNullable(newClock).orElse(Clock.systemUTC());
    CACHE.clear();
  }

  /** Clears the in-memory cache. */
  public static void resetCache() {
    CACHE.clear();
  }

  /** Also clears cache; next access will lazily rebuild a client with current config. */
  public static synchronized void resetClient() {
    Optional.ofNullable(client)
        .ifPresent(
            c -> {
              try {
                c.close();
              } catch (final Exception ignored) {
              }
            });
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
    Optional.ofNullable(System.getProperty("aws.accessKeyId", System.getenv("AWS_ACCESS_KEY_ID")))
        .flatMap(
            accessKey ->
                Optional.ofNullable(
                        System.getProperty(
                            "aws.secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY")))
                    .map(secretKey -> AwsBasicCredentials.create(accessKey, secretKey)))
        .map(StaticCredentialsProvider::create)
        .ifPresentOrElse(
            builder::credentialsProvider,
            () -> builder.credentialsProvider(DefaultCredentialsProvider.builder().build()));

    return builder.build();
  }

  /** Lazily gets the SecretsManagerClient, building it if necessary. */
  static synchronized SecretsManagerClient getClient() {
    return Optional.ofNullable(client).orElseGet(() -> client = buildClient());
  }

  /**
   * Retrieves the raw secret string for the given secret identifier. Uses cache when enabled (ttl >
   * 0).
   *
   * @param secretId the Secrets Manager secret ID or name
   * @return the secret string as stored in Secrets Manager
   */
  public static String getSecret(final String secretId) {
    return getSecretValue(secretId, CacheEntry::secretString, GetSecretValueResponse::secretString);
  }

  /**
   * Retrieves the current version identifier for the given secret. Uses cache when enabled (ttl >
   * 0).
   *
   * @param secretId the Secrets Manager secret ID or name
   * @return versionId of the latest secret value
   */
  public static String getSecretVersion(final String secretId) {
    return getSecretValue(secretId, CacheEntry::versionId, GetSecretValueResponse::versionId);
  }

  private static String getSecretValue(
      final String secretId,
      final Function<CacheEntry, String> cacheExtractor,
      final Function<GetSecretValueResponse, String> responseExtractor) {
    return Optional.of(ttlMillis)
        .filter(ttl -> ttl > 0)
        .map(
            ttl -> {
              final var now = Instant.now(clock).toEpochMilli();
              return Optional.ofNullable(CACHE.get(secretId))
                  .filter(cached -> cached.expiresAtMillis >= now)
                  .map(cacheExtractor)
                  .orElseGet(
                      () -> {
                        final var response = fetchSecret(secretId);
                        final var entry =
                            new CacheEntry(
                                response.secretString(), response.versionId(), now + ttl);
                        CACHE.put(secretId, entry);
                        return cacheExtractor.apply(entry);
                      });
            })
        .orElseGet(() -> responseExtractor.apply(fetchSecret(secretId)));
  }

  private static GetSecretValueResponse fetchSecret(final String secretId) {
    final var request = GetSecretValueRequest.builder().secretId(secretId).build();
    return getClient().getSecretValue(request);
  }

  private record CacheEntry(String secretString, String versionId, long expiresAtMillis) {}
}
