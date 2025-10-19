package com.example.rotating.datasource.core.secrets;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SecretHelperTest {

  private ObjectMapper testMapper;

  @BeforeEach
  void setup() {
    testMapper = new ObjectMapper();
    testMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    SecretHelper.setMapperSupplier(() -> testMapper);
  }

  @AfterEach
  void cleanup() {
    System.clearProperty("aws.region");
    System.clearProperty("aws.sm.endpoint");
    SecretsManagerProvider.resetClient();
    SecretHelper.setMapperSupplier(ObjectMapper::new);
  }

  @Test
  void shouldParseValidSecretJson() throws JsonProcessingException {
    final var secret =
        new DbSecret("testuser", "testpass", "postgres", "localhost", 5432, "testdb");
    final var json = testMapper.writeValueAsString(secret);
    final var parsed = testMapper.readValue(json, DbSecret.class);
    assertEquals(secret, parsed);
  }

  @Test
  void shouldHandleJsonWithExtraFields() throws JsonProcessingException {
    final var json =
        """
        {
          "username": "testuser",
          "password": "testpass",
          "engine": "postgres",
          "host": "localhost",
          "port": 5432,
          "dbname": "testdb",
          "extra_field": "ignored"
        }
        """;

    final var parsed = testMapper.readValue(json, DbSecret.class);

    assertEquals("testuser", parsed.username());
    assertEquals("testpass", parsed.password());
    assertEquals("postgres", parsed.engine());
    assertEquals("localhost", parsed.host());
    assertEquals(5432, parsed.port());
    assertEquals("testdb", parsed.dbname());
  }

  @Test
  void shouldFailOnInvalidJson() {
    assertThrows(
        JsonProcessingException.class, () -> testMapper.readValue("invalid json", DbSecret.class));
  }

  @Test
  void shouldAllowPartialJsonForRecords() {
    final var json =
        """
        {
          "username": "testuser"
        }
        """;

    assertDoesNotThrow(
        () -> {
          var parsed = testMapper.readValue(json, DbSecret.class);
          assertEquals("testuser", parsed.username());
          assertNull(parsed.password());
          assertNull(parsed.engine());
        });
  }

  @Test
  void shouldUseConfiguredMapperSupplier() {
    final var strictMapper = new ObjectMapper();
    strictMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    SecretHelper.setMapperSupplier(() -> strictMapper);

    var json =
        """
        {
          "username": "testuser",
          "password": "testpass",
          "engine": "postgres",
          "host": "localhost",
          "port": 5432,
          "dbname": "testdb",
          "extra_field": "should_fail"
        }
        """;

    assertThrows(JsonProcessingException.class, () -> strictMapper.readValue(json, DbSecret.class));
  }
}
