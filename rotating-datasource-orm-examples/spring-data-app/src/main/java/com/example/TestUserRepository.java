package com.example;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface TestUserRepository extends JpaRepository<TestUser, Long> {
  Optional<TestUser> findFirstByUsernameStartingWithOrderByIdAsc(String prefix);

  @Modifying(clearAutomatically = true, flushAutomatically = true)
  @Transactional
  @Query(
      value =
          "DELETE FROM test_users WHERE id IN (SELECT id FROM test_users WHERE username LIKE :prefix || '%' ORDER BY id LIMIT 1)",
      nativeQuery = true)
  int deleteOneByUsernamePrefix(@Param("prefix") String prefix);
}
