package com.example.rotating.datasource.core.secrets;

/**
 * Representation of a database secret stored in AWS Secrets Manager.
 *
 * <p>Fields map directly to the common RDS secret JSON structure.
 *
 * @param username database username
 * @param password database password
 * @param engine database engine identifier (e.g., postgres, mysql)
 * @param host database host name or address
 * @param port database port number
 * @param dbname database name/schema
 */
public record DbSecret(
    String username, String password, String engine, String host, int port, String dbname) {}
