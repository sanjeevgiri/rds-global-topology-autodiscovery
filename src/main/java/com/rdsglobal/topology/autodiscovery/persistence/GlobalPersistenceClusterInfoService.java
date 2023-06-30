package com.rdsglobal.topology.autodiscovery.persistence;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.model.GlobalCluster;
import io.vavr.control.Try;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
public class GlobalPersistenceClusterInfoService {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPersistenceClusterInfoService.class);
  private final GlobalPersistenceClusterEndpoints bootTimeDbClusterEndpoints;
  private final AmazonRDS amazonRdsGlobalClient;
  private final GlobalPersistenceClusterProperties props;
  private final GlobalPersistenceClusterEndpoints emptyRuntimeDbClusterEndpoints;
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

    emptyRuntimeDbClusterEndpoints = GlobalPersistenceClusterEndpoints.builder()
      .globalClusterIdentifier(bootTimeDbClusterEndpoints.getGlobalClusterIdentifier())
      .readerJdbcUrl("")
      .writerJdbcUrl("")
      .build();
  }

  public GlobalPersistenceClusterEndpoints getBootTimeDbClusterEndpoints() {
    return bootTimeDbClusterEndpoints;
  }

  public GlobalPersistenceClusterEndpoints getRunTimeDbClusterEndpoints() {
    return runTimeDbClusterEndpoints;
  }

  @Scheduled(fixedDelay = 1, initialDelay = 1, timeUnit = TimeUnit.MINUTES)
  public void refreshRunTimeDbClusterEndpoints() {

    // There could be failure while trying to fetch global cluster details
    Try<GlobalCluster> globalCluster = Try
      .of(() -> GlobalPersistenceClusterUtil.globalCluster(amazonRdsGlobalClient, props.getGlobalClusterId()))
      .map(Optional::get);

    // fallback to last known config if rate limited / throttled down, fallback to empty is global cluster is memberless
    Supplier<GlobalPersistenceClusterEndpoints> fallbackRunTimeDbClusterEndpoints = () -> globalCluster
      .map(gc -> !CollectionUtils.isEmpty(gc.getGlobalClusterMembers()) ? runTimeDbClusterEndpoints :
        emptyRuntimeDbClusterEndpoints)
      .getOrElse(runTimeDbClusterEndpoints);

    runTimeDbClusterEndpoints = globalCluster
      .map(gc -> GlobalPersistenceClusterUtil.globalClusterEndpoints(gc, props))
      .onFailure(e -> LOGGER.error("Encountered error while evaluating global db cluster topology", e))
      .getOrElse(fallbackRunTimeDbClusterEndpoints);

    LOGGER.info("""
              
        ********************** Refreshed RunTime DB Cluster Endpoints **************************************
        Boottime config: {}
        Runtime config: {}
        In Sync: {}
        ********************** Refreshed RunTime DB Cluster Endpoints **************************************
              
        """,
      bootTimeDbClusterEndpoints,
      runTimeDbClusterEndpoints,
      isBootTimeAndRunTimeClusterTopologyInSync()
    );
  }

  public boolean isBootTimeAndRunTimeClusterTopologyInSync() {
    return Optional.of(bootTimeDbClusterEndpoints)
      .filter(bootimeDbCfg -> bootimeDbCfg.getReaderJdbcUrl().equals(runTimeDbClusterEndpoints.getReaderJdbcUrl()))
      .filter(bootimeDbCfg -> bootimeDbCfg.getWriterJdbcUrl().equals(runTimeDbClusterEndpoints.getWriterJdbcUrl()))
      .isPresent();
  }
}
