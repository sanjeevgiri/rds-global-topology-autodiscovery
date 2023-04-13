package com.rdsglobal.topology.autodiscovery.health;

import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/health")
public class HealthController {
  public static final Logger LOGGER = LoggerFactory.getLogger(HealthController.class);
  private final HealthService service;

  public HealthController(HealthService service) {
    this.service = service;
  }

  @GetMapping
  public ResponseEntity<HealthResource> get() {
    HealthResource health = service.get();
    Consumer<HealthResource> nonOkStatusLogger = h -> LOGGER.info("""
        
      ************** DETECTED HEALTH DOWN **********************
      Heahth Check Status:
      {}
      ************** DETECTED HEALTH DOWN **********************
            
      """, h);

    Optional.of(health)
      .filter(h -> !HttpStatus.OK.equals(h.getStatus()))
      .ifPresent(nonOkStatusLogger);


    return ResponseEntity
      .status(health.getStatus())
      .body(health);
  }
}
