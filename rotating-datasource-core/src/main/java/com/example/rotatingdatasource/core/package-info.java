/**
 * Root package for the sm-rotator library.
 *
 * <p>This package contains a small set of focused classes that build a {@link javax.sql.DataSource
 * DataSource} which can be refreshed from credentials stored in AWS Secrets Manager, and a tiny
 * JDBC client that retries once on authentication failures.
 *
 * <p>Package contents:
 *
 * <ul>
 *   <li>{@link com.example.rotatingdatasource.core.secrets.DbSecret} – immutable representation of
 *       a database secret payload.
 *   <li>{@link com.example.rotatingdatasource.core.secrets.SecretsManagerProvider} – lazily
 *       configured AWS Secrets Manager client (supports endpoint/region/credentials overrides).
 *   <li>{@link com.example.rotatingdatasource.core.secrets.SecretHelper} – fetches raw secret JSON
 *       and deserializes to {@code DbSecret}.
 *   <li>{@link com.example.rotatingdatasource.core.jdbc.DataSourceFactoryProvider} – functional
 *       factory for building a DataSource from a secret.
 *   <li>{@link com.example.rotatingdatasource.core.jdbc.RotatingDataSource} – wraps a DataSource
 *       and recreates it on demand.
 *   <li>{@link com.example.rotatingdatasource.core.jdbc.Retry} – minimal retry helper used to
 *       express retry-on-exception.
 *   <li>{@link com.example.rotatingdatasource.core.jdbc.DbClient} – executes JDBC operations and
 *       retries once on auth failures.
 *   <li>{@link com.example.rotatingdatasource.core.reactive.ConnectionFactoryProvider} – functional
 *       factory for building a reactive {@code ConnectionFactory} from a secret.
 *   <li>{@link com.example.rotatingdatasource.core.reactive.RotatingConnectionFactory} – wraps a
 *       reactive {@code ConnectionFactory} and recreates it on demand.
 *   <li>{@link com.example.rotatingdatasource.core.reactive.R2dbcRetry} – minimal retry and error
 *       detection utilities for R2DBC.
 * </ul>
 */
package com.example.rotatingdatasource.core;
