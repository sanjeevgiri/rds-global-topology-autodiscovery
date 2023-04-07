package com.rdsglobal.topology.autodiscovery.shop;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ShopResource {
  private String domain;
  private String name;
  
  public ShopResource(String domain, String name) {
    this.domain = domain;
    this.name = name;
  }
}
