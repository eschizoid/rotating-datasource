/**
 * Core module for AWS Secrets Manager database credential rotation.
 *
 * <p>Provides components for:
 *
 * <ul>
 *   <li>Fetching database secrets from AWS Secrets Manager
 *   <li>Creating rotating DataSource wrappers
 *   <li>Retry logic for transient failures and authentication errors
 *   <li>JDBC client with automatic credential rotation
 * </ul>
 */
module com.example.smrotator.core {
  requires java.sql;
  requires java.logging;
  requires r2dbc.spi;
  requires reactor.core;
  requires software.amazon.awssdk.auth;
  requires software.amazon.awssdk.regions;
  requires software.amazon.awssdk.services.secretsmanager;
  requires com.fasterxml.jackson.databind;

  exports com.example.rotatingdatasource.core;
}
