package com.example.rotatingdatasource.core;

import io.r2dbc.spi.ConnectionFactory;

@FunctionalInterface
public interface ConnectionFactoryProvider {
  ConnectionFactory create(DbSecret secret);
}
