package com.rdsglobal.topology.autodiscovery.persistence;

import com.amazonaws.services.rds.AmazonRDS;
import io.vavr.control.Try;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class GlobalPersistenceClusterInfoService {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPersistenceClusterInfoService.class);
  private final GlobalPersistenceClusterEndpoints bootTimeDbClusterEndpoints;
  private final AmazonRDS amazonRdsGlobalClient;
  private final GlobalPersistenceClusterProperties props;
  private GlobalPersistenceClusterEndpoints runTimeDbClusterEndpoints;

  public GlobalPersistenceClusterInfoService(
    GlobalPersistenceClusterEndpoints bootTimeDbClusterEndpoints,
    AmazonRDS amazonRdsGlobalClient,
    GlobalPersistenceClusterProperties props
  ) {
    this.bootTimeDbClusterEndpoints = bootTimeDbClusterEndpoints;
    this.amazonRdsGlobalClient = amazonRdsGlobalClient;
    this.props = props;
    runTimeDbClusterEndpoints = GlobalPersistenceClusterEndpoints.builder()
      .globalClusterIdentifier(bootTimeDbClusterEndpoints.getGlobalClusterIdentifier())
      .readerJdbcUrl(bootTimeDbClusterEndpoints.getReaderJdbcUrl())
      .writerJdbcUrl(bootTimeDbClusterEndpoints.getWriterJdbcUrl())
      .build();
  }

  public GlobalPersistenceClusterEndpoints getBootTimeDbClusterEndpoints() {
    return bootTimeDbClusterEndpoints;
  }

  public GlobalPersistenceClusterEndpoints getRunTimeDbClusterEndpoints() {
    return runTimeDbClusterEndpoints;
  }

  @Scheduled(fixedDelay = 2, initialDelay = 2, timeUnit = TimeUnit.MINUTES)
  public void refreshRunTimeDbClusterEndpoints() {
    this.runTimeDbClusterEndpoints = Try
      .of(() -> GlobalPersistenceClusterUtil.globalClusterEndpoints(amazonRdsGlobalClient, props))
      .onFailure(e -> LOGGER.error("Encountered error while evaluating global db cluster topology", e))
      .getOrElse(runTimeDbClusterEndpoints); // fallback to last known config if rate limited / throttled down

    LOGGER.info("""
              
        ********************** Refresed RunTime DB Cluster Endpoints **************************************
        Boottime config: {}
        Runtime config: {}
        In Sync: {}
        ********************** Refresed RunTime DB Cluster Endpoints **************************************
              
        """,
      bootTimeDbClusterEndpoints,
      runTimeDbClusterEndpoints,
      bootTimeDbClusterEndpoints.equals(runTimeDbClusterEndpoints)
    );
  }
}
