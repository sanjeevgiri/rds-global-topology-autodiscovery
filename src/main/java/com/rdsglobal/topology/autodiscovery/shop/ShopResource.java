package com.rdsglobal.topology.autodiscovery.shop;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ShopResource {
  private String id;
  private String domain;
  private String name;
  
  public ShopResource(String id, String domain, String name) {
    this.id = id;
    this.domain = domain;
    this.name = name;
  }
}
