package com.rdsglobal.topology.autodiscovery.health;

import com.rdsglobal.topology.autodiscovery.persistence.GlobalPersistenceClusterEndpoints;
import com.rdsglobal.topology.autodiscovery.persistence.GlobalPersistenceClusterInfoService;
import java.util.Optional;
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
    GlobalPersistenceClusterEndpoints boottimeDb = globalPersistenceClusterInfoService.getBootTimeDbClusterEndpoints();
    GlobalPersistenceClusterEndpoints runtimeDb = globalPersistenceClusterInfoService.getRunTimeDbClusterEndpoints();

    return Optional.of(boottimeDb)
      .filter(bootimeDbCfg -> bootimeDbCfg.getReaderJdbcUrl().equals(runtimeDb.getReaderJdbcUrl()))
      .filter(bootimeDbCfg -> bootimeDbCfg.getWriterJdbcUrl().equals(runtimeDb.getWriterJdbcUrl()))
      .map(any -> HttpStatus.OK)
      .orElse(HttpStatus.SERVICE_UNAVAILABLE);
  }
}
