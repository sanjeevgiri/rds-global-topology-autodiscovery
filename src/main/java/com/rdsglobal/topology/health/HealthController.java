package com.rdsglobal.topology.health;

import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/health")
public class HealthController {
  private final HealthService service;

  public HealthController(HealthService service) {
    this.service = service;
  }

  @GetMapping
  public ResponseEntity<HealthResource> get() {
    HealthResource health = service.get();
    return ResponseEntity
      .status(health.getStatus())
      .body(health);
  }
}
