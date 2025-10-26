package com.example;

import java.time.Instant;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

public interface TestRepository extends Repository<TestEntity, Long> {
  @Query(value = "SELECT now()", nativeQuery = true)
  Instant now();
}
