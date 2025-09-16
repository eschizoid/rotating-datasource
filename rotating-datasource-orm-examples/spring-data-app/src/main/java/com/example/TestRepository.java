package com.example;

import java.time.Instant;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

/** Spring Data repository exposing a simple native function to get DB time. */
public interface TestRepository extends Repository<TestEntity, Long> {
  @Query(value = "SELECT now()", nativeQuery = true)
  Instant now();
}
