/**
 * Root package for the sm-rotator library.
 *
 * <p>This package contains a small set of focused classes that demonstrate how to build a {@link
 * javax.sql.DataSource DataSource} which can be refreshed from credentials stored in AWS Secrets
 * Manager, and a tiny JDBC client that retries once on authentication failures.
 *
 * <p>Package contents:
 *
 * <ul>
 *   <li>{@link com.example.smrotator.core.DbSecret} – immutable representation of a database secret
 *       payload.
 *   <li>{@link com.example.smrotator.core.SecretsManagerProvider} – lazily configured AWS Secrets
 *       Manager client (supports endpoint/region/credentials overrides).
 *   <li>{@link com.example.smrotator.core.SecretHelper} – fetches raw secret JSON and deserializes
 *       to {@code DbSecret}.
 *   <li>{@link com.example.smrotator.core.DataSourceFactory} – functional factory for building a
 *       DataSource from a secret.
 *   <li>{@link com.example.smrotator.core.RotatingDataSource} – wraps a DataSource and recreates it
 *       on demand.
 *   <li>{@link com.example.smrotator.core.Retry} – minimal retry helper used to express
 *       retry-on-exception.
 *   <li>{@link com.example.smrotator.core.DbClient} – executes JDBC operations and retries once on
 *       auth failures.
 * </ul>
 *
 * <h3>Naming and repackaging guidance</h3>
 *
 * <p>The current names are intentionally concise and match their responsibilities. Given the
 * library's small size, introducing subpackages (e.g., {@code .aws}, {@code .jdbc}, {@code .util})
 * would add indirection without clear benefit. If the codebase grows, we should consider a future
 * split:
 *
 * <ul>
 *   <li>{@code com.example.secrets} – providers/helpers for Secrets Manager and secret models.
 *   <li>{@code com.example.datasource} – rotating data source and factory abstractions.
 *   <li>{@code com.example.jdbc} – the DbClient and retry policies.
 * </ul>
 */
package com.example.smrotator.core;
