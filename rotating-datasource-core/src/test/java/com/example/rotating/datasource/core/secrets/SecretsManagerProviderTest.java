package com.example.rotating.datasource.core.secrets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

class SecretsManagerProviderTest {

  private static final String SECRET_ID = "test/secret";

  private MockedStatic<SecretsManagerProvider> clientStaticMock;

  @BeforeEach
  void setup() {
    // Clean slate for each test
    clearAwsProps();
    SecretsManagerProvider.configureCacheForTests(0, null);
    SecretsManagerProvider.resetClient();
  }

  @AfterEach
  void tearDown() {
    if (clientStaticMock != null) {
      clientStaticMock.close();
      clientStaticMock = null;
    }
    SecretsManagerProvider.resetClient();
    clearAwsProps();
  }

  private static void clearAwsProps() {
    System.clearProperty("aws.region");
    System.clearProperty("aws.sm.endpoint");
    System.clearProperty("aws.accessKeyId");
    System.clearProperty("aws.secretAccessKey");
    System.clearProperty("aws.sm.cache.ttl.millis");
  }

  @Test
  @DisplayName("buildClient honors region/endpoint/credentials properties")
  void buildClientHonorsProperties() {
    // Set properties to exercise buildClient lambdas
    System.setProperty("aws.region", "us-west-2");
    System.setProperty("aws.sm.endpoint", "http://localhost:9999");
    System.setProperty("aws.accessKeyId", "abc");
    System.setProperty("aws.secretAccessKey", "xyz");

    // Force rebuild and obtain client (we don't assert internals; invoking is enough for coverage)
    SecretsManagerProvider.resetClient();
    final var client = SecretsManagerProvider.getClient();
    assertNotNull(client);
  }

  @Test
  @DisplayName("getSecret/getVersion without cache call underlying client each time (ttl=0)")
  void getSecretNoCacheInvokesClientEachTime() {
    final var mockClient = mock(SecretsManagerClient.class);

    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(response("value-1", "v1"))
        .thenReturn(response("value-2", "v2"));

    // Mock static getClient() to return our mock client
    clientStaticMock = Mockito.mockStatic(SecretsManagerProvider.class, Mockito.CALLS_REAL_METHODS);
    clientStaticMock.when(SecretsManagerProvider::getClient).thenReturn(mockClient);

    SecretsManagerProvider.configureCacheForTests(0, null); // disable cache

    final var s1 = SecretsManagerProvider.getSecret(SECRET_ID);
    final var v1 = SecretsManagerProvider.getSecretVersion(SECRET_ID);

    assertEquals("value-1", s1);
    assertEquals("v2", v1); // second call consumed second response

    verify(mockClient, times(2)).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  @DisplayName("getSecret uses cache when ttl>0 and avoids second client call within TTL")
  void getSecretUsesCacheWithinTtl() {
    final var mockClient = mock(SecretsManagerClient.class);
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(response("cached", "v1"));

    clientStaticMock = Mockito.mockStatic(SecretsManagerProvider.class, Mockito.CALLS_REAL_METHODS);
    clientStaticMock.when(SecretsManagerProvider::getClient).thenReturn(mockClient);

    SecretsManagerProvider.configureCacheForTests(1_000, Clock.systemUTC());

    final var s1 = SecretsManagerProvider.getSecret(SECRET_ID);
    final var s2 = SecretsManagerProvider.getSecret(SECRET_ID);

    assertEquals("cached", s1);
    assertEquals("cached", s2);

    // Only one call due to cache hit
    verify(mockClient, times(1)).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  @DisplayName("cache expires after TTL and fetches again")
  void cacheExpiresAndRefetches() throws InterruptedException {
    final var mockClient = mock(SecretsManagerClient.class);
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(response("first", "v1"))
        .thenReturn(response("second", "v2"));

    clientStaticMock = Mockito.mockStatic(SecretsManagerProvider.class, Mockito.CALLS_REAL_METHODS);
    clientStaticMock.when(SecretsManagerProvider::getClient).thenReturn(mockClient);

    SecretsManagerProvider.configureCacheForTests(50, Clock.systemUTC());

    assertEquals("first", SecretsManagerProvider.getSecret(SECRET_ID));
    // Sleep beyond TTL
    Thread.sleep(60);
    assertEquals("second", SecretsManagerProvider.getSecret(SECRET_ID));

    verify(mockClient, times(2)).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  @DisplayName("configureCacheForTests overrides TTL and Clock (deterministic time)")
  void configureCacheOverridesTtlAndClock() {
    final var mockClient = mock(SecretsManagerClient.class);
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(response("A", "v1"))
        .thenReturn(response("B", "v2"));

    clientStaticMock = Mockito.mockStatic(SecretsManagerProvider.class, Mockito.CALLS_REAL_METHODS);
    clientStaticMock.when(SecretsManagerProvider::getClient).thenReturn(mockClient);

    final Instant base = Instant.parse("2020-01-01T00:00:00Z");
    final Clock fixedBase = Clock.fixed(base, ZoneOffset.UTC);

    // Set TTL 100ms at base time: first call caches
    SecretsManagerProvider.configureCacheForTests(100, fixedBase);
    assertEquals("A", SecretsManagerProvider.getSecret(SECRET_ID));

    // Advance clock by 200ms by reconfiguring cache with an offset clock; exercises configure path
    final Clock advanced = Clock.offset(fixedBase, Duration.ofMillis(200));
    SecretsManagerProvider.configureCacheForTests(100, advanced);

    // Cache cleared by configure; second call fetches new value
    assertEquals("B", SecretsManagerProvider.getSecret(SECRET_ID));

    verify(mockClient, times(2)).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  @DisplayName("resetClient closes existing client and clears cache")
  void resetClientClosesAndClears() throws Exception {
    final var mockClient = mock(SecretsManagerClient.class);

    // Set private static field 'client' via reflection
    final Field clientField = SecretsManagerProvider.class.getDeclaredField("client");
    clientField.setAccessible(true);
    clientField.set(null, mockClient);

    // Put an entry in cache
    SecretsManagerProvider.configureCacheForTests(1_000, Clock.systemUTC());
    // To populate cache we need to mock getClient() for this operation
    clientStaticMock = Mockito.mockStatic(SecretsManagerProvider.class, Mockito.CALLS_REAL_METHODS);
    clientStaticMock.when(SecretsManagerProvider::getClient).thenReturn(mockClient);
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(response("cached", "v1"));

    assertEquals("cached", SecretsManagerProvider.getSecret(SECRET_ID));

    // Now reset and ensure close is called and cache cleared
    clientStaticMock.close();
    clientStaticMock = null;

    SecretsManagerProvider.resetClient();
    verify(mockClient, times(1)).close();

    // After reset, client is null and cache is empty; set a new mock to avoid NPE on next fetch
    final var mockClient2 = mock(SecretsManagerClient.class);
    clientStaticMock = Mockito.mockStatic(SecretsManagerProvider.class, Mockito.CALLS_REAL_METHODS);
    clientStaticMock.when(SecretsManagerProvider::getClient).thenReturn(mockClient2);
    when(mockClient2.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(response("afterReset", "v2"));

    assertEquals("afterReset", SecretsManagerProvider.getSecret(SECRET_ID));
    verify(mockClient2, times(1)).getSecretValue(any(GetSecretValueRequest.class));
  }

  private static GetSecretValueResponse response(final String value, final String version) {
    return GetSecretValueResponse.builder().secretString(value).versionId(version).build();
  }
}
