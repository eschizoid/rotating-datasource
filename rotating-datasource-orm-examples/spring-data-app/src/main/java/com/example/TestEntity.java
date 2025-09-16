package com.example;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Minimal entity to satisfy Spring Data JPA repository typing. */
@Entity
@Table(name = "test")
public class TestEntity {
  @Id private Long id;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }
}
