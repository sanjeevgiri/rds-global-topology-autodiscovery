package com.rdsglobal.topology.autodiscovery.health;

import com.rdsglobal.topology.autodiscovery.persistence.GlobalPersistenceClusterInfoService;
import org.springframework.boot.info.BuildProperties;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class HealthService {
  private final BuildProperties buildInfo;
  private final GlobalPersistenceClusterInfoService globalPersistenceClusterInfoService;

  public HealthService(
    BuildProperties buildInfo,
    GlobalPersistenceClusterInfoService globalPersistenceClusterInfoService
  ) {
    this.buildInfo = buildInfo;
    this.globalPersistenceClusterInfoService = globalPersistenceClusterInfoService;
  }

  public HealthResource get() {
    return HealthResource.builder()
      .status(evaluateStatus())
      .builInfo(buildInfo)
      .bootTimeDbClusterEndpoints(globalPersistenceClusterInfoService.getBootTimeDbClusterEndpoints())
      .runtTimeDbClusterEndpoints(globalPersistenceClusterInfoService.getRunTimeDbClusterEndpoints())
      .build();
  }

  private HttpStatus evaluateStatus() {
    return globalPersistenceClusterInfoService.isBootTimeAndRunTimeClusterTopologyInSync() ?
      HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
  }
}
