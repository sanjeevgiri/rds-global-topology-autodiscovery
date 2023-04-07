package com.rdsglobal.topology.autodiscovery.shop;

import java.util.List;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ShopService {
  private final ShopConfigRepository repo;

  public ShopService(ShopConfigRepository repo) {
    this.repo = repo;
  }

  @Transactional(transactionManager = "readerTransactionManager", readOnly = true)
  public List<ShopResource> list() {
    return repo.findAll().stream()
      .map(s -> new ShopResource(s.getDomain(), s.getName()))
      .collect(Collectors.toList());
  }

  @Transactional(transactionManager = "writerTransactionManager", readOnly = false)
  public void create(ShopResource resource) {
    repo.save(new Shop(resource.getDomain(), resource.getName()));
  }

  @Transactional(transactionManager = "writerTransactionManager", readOnly = false)
  public void update(String domain, String name) {
    repo.save(new Shop(domain, name));
  }
}
