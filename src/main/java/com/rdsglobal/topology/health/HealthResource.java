package com.rdsglobal.topology.health;

import com.rdsglobal.topology.autodiscovery.persistence.GlobalPersistenceClusterEndpoints;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.springframework.boot.info.BuildProperties;
import org.springframework.http.HttpStatus;

@Getter
@ToString
@Builder
public class HealthResource {
  private final HttpStatus status;
  private GlobalPersistenceClusterEndpoints bootTimeDbClusterEndpoints;
  private GlobalPersistenceClusterEndpoints runtTimeDbClusterEndpoints;
  private BuildProperties builInfo;
}
