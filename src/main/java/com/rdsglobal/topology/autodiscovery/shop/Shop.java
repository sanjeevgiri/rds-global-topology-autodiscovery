package com.rdsglobal.topology.autodiscovery.shop;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class Shop {
  @Id
  private String domain;
  private String name;

  public Shop(String domain, String name) {
    this.domain = domain;
    this.name = name;
  }
}
