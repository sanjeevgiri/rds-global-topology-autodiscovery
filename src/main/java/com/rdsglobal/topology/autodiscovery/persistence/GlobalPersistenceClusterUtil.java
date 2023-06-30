package com.rdsglobal.topology.autodiscovery.persistence;

import com.amazonaws.arn.Arn;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.CreateGlobalClusterRequest;
import com.amazonaws.services.rds.model.DBCluster;
import com.amazonaws.services.rds.model.DBClusterEndpoint;
import com.amazonaws.services.rds.model.DeleteGlobalClusterRequest;
import com.amazonaws.services.rds.model.DescribeDBClusterEndpointsRequest;
import com.amazonaws.services.rds.model.DescribeDBClusterEndpointsResult;
import com.amazonaws.services.rds.model.DescribeDBClustersRequest;
import com.amazonaws.services.rds.model.DescribeGlobalClustersRequest;
import com.amazonaws.services.rds.model.DescribeGlobalClustersResult;
import com.amazonaws.services.rds.model.GlobalCluster;
import com.amazonaws.services.rds.model.GlobalClusterMember;
import com.amazonaws.services.rds.model.GlobalClusterNotFoundException;
import com.zaxxer.hikari.HikariConfig;
import io.vavr.control.Try;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

public final class GlobalPersistenceClusterUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPersistenceClusterUtil.class.getName());

  private GlobalPersistenceClusterUtil() {
  }

  public static GlobalPersistenceClusterEndpoints globalClusterEndpoints(
    GlobalCluster globalCluster, GlobalPersistenceClusterProperties props
  ) {
    GlobalClusterMember writer = writerCluster(globalCluster.getGlobalClusterMembers());
    GlobalClusterMember reader = readerCluster(globalCluster.getGlobalClusterMembers(), props.getClientAppRegion());
    AmazonRDS writerRdsClient = AmazonRDSClientBuilder.standard()
      .withRegion(Arn.fromString(writer.getDBClusterArn()).getRegion())
      .build();

    AmazonRDS readerRdsClient = AmazonRDSClientBuilder.standard()
      .withRegion(Arn.fromString(reader.getDBClusterArn()).getRegion())
      .build();

    String writerEndpoint = clusterEndpoint(writerRdsClient, writer, GlobalPersistenceClusterEndpointType.WRITER)
      .orElseThrow(() -> new RuntimeException("Unable to find endpoint for writer " + writer.getDBClusterArn()));
    String readerEndpoint = clusterEndpoint(readerRdsClient, reader, GlobalPersistenceClusterEndpointType.READER)
      .orElse(writerEndpoint);

    return GlobalPersistenceClusterEndpoints.builder()
      .globalClusterIdentifier(props.getGlobalClusterId())
      .readerJdbcUrl(jdbcUrl(readerEndpoint, props.getPort(), props.getName()))
      .writerJdbcUrl(jdbcUrl(writerEndpoint, props.getPort(), props.getName()))
      .build();
  }

  public static GlobalPersistenceClusterEndpoints globalClusterEndpoints(
    AmazonRDS rdsGlobalClient, GlobalPersistenceClusterProperties props
  ) {
    GlobalCluster globalCluster = null;
    Optional<GlobalCluster> globalClusterOpt = globalCluster(rdsGlobalClient, props.getGlobalClusterId());
    Supplier<DBCluster> preferredWriter = () -> preferredWriterDbCluster(props);

    Supplier<GlobalCluster> whenGlobalClusterNotFound = () -> {
      LOGGER.error("Cluster {} not found, creating from {}", props.getGlobalClusterId(), props.getClientAppRegion());
      DBCluster writer = preferredWriter.get();
      return reconfiguredGlobalMemberlessClusterWithPreferredWriter(rdsGlobalClient, props, writer);
    };

    Supplier<GlobalCluster> whenMemberlessGlobalClusterDetected = () -> {
      LOGGER.error("Detected memberless global cluster {}", props.getGlobalClusterId());
      DBCluster writer = preferredWriter.get();
      rdsGlobalClient
        .deleteGlobalCluster(new DeleteGlobalClusterRequest().withGlobalClusterIdentifier(props.getGlobalClusterId()));
      return reconfiguredGlobalMemberlessClusterWithPreferredWriter(rdsGlobalClient, props, writer);
    };

    if (globalClusterOpt.isEmpty()) {
      globalCluster = Try.of(whenGlobalClusterNotFound::get)
        .onFailure(e -> LOGGER.error("Encountered error attempting to recover undetected global cluster", e))
        .get();
    } else if (CollectionUtils.isEmpty(globalClusterOpt.get().getGlobalClusterMembers())) {
      globalCluster = Try.of(whenMemberlessGlobalClusterDetected::get)
        .onFailure(e -> LOGGER.error("Encountered error attempting to recover memberless global cluster", e))
        .get();
    } else {
      globalCluster = globalClusterOpt.get();
    }

    return globalClusterEndpoints(globalCluster, props);
  }

  private static DBCluster preferredWriterDbCluster(GlobalPersistenceClusterProperties props) {
    AmazonRDS preferredWriterRdsClient = AmazonRDSClientBuilder.standard()
      .withRegion(props.getClientAppRegion())
      .build();
    DescribeDBClustersRequest req = new DescribeDBClustersRequest()
      .withDBClusterIdentifier(props.getGlobalMemberlessClusterPrefferedWriter());

    Consumer<Throwable> logError = e -> LOGGER.error(
      "Unable to find preferred writer {} in region {}",
      props.getGlobalMemberlessClusterPrefferedWriter(),
      props.getClientAppRegion()
    );

    return Try.of(() -> preferredWriterRdsClient.describeDBClusters(req).getDBClusters())
      .map(dbClusters -> dbClusters.stream().findFirst())
      .map(Optional::get)
      .onFailure(logError)
      .get();
  }

  private static GlobalCluster reconfiguredGlobalMemberlessClusterWithPreferredWriter(
    AmazonRDS rdsGlobalClient, GlobalPersistenceClusterProperties props, DBCluster dbCluster
  ) {
    Supplier<String> errorMessage = () -> "Unable to create global custer "
                                          + props.getGlobalClusterId()
                                          + " from region "
                                          + props.getClientAppRegion()
                                          + ": ";

    Supplier<GlobalCluster> globalCluster = () -> {
      LOGGER.info("Recreating global rds global cluster");
      return rdsGlobalClient.createGlobalCluster(new CreateGlobalClusterRequest()
        .withGlobalClusterIdentifier(props.getGlobalClusterId())
        .withSourceDBClusterIdentifier(dbCluster.getDBClusterArn())
      );
    };

    return Try.of(globalCluster::get)
      .onFailure(e -> LOGGER.error(errorMessage.get(), e))
      .get();
  }

  public static Optional<GlobalCluster> globalCluster(AmazonRDS amazonRdsGlobalClient, String globalClusterIdentifier) {
    DescribeGlobalClustersRequest descGlobalClusterReq = new DescribeGlobalClustersRequest();
    descGlobalClusterReq.setGlobalClusterIdentifier(globalClusterIdentifier);

    return Try.of(() -> amazonRdsGlobalClient.describeGlobalClusters(descGlobalClusterReq))
      .map(DescribeGlobalClustersResult::getGlobalClusters)
      .onFailure(e -> LOGGER.error("Error while getting global cluster " + globalClusterIdentifier, e))
      .recoverWith(GlobalClusterNotFoundException.class, e -> Try.of(Collections::emptyList))
      .get()
      .stream()
      .findFirst();
  }

  private static String jdbcUrl(String endpoint, String port, String database) {
    return String.format("jdbc:aws-wrapper:postgresql://%s:%s/%s", endpoint, port, database);
  }

  private static GlobalClusterMember writerCluster(List<GlobalClusterMember> members) {
    return members.stream().filter(GlobalClusterMember::isWriter)
      .findFirst()
      .orElseThrow(() -> new RuntimeException("Unable to determine writer"));
  }

  private static GlobalClusterMember readerCluster(List<GlobalClusterMember> members, String region) {
    return members.stream().filter(m -> Arn.fromString(m.getDBClusterArn()).getRegion().equals(region))
      .findFirst()
      .orElseGet(() -> writerCluster(members));
  }

  private static Optional<String> clusterEndpoint(
    AmazonRDS rdsClient, GlobalClusterMember member, GlobalPersistenceClusterEndpointType type
  ) {
    Arn clusterArn = Arn.fromString(member.getDBClusterArn());

    DescribeDBClusterEndpointsRequest req = new DescribeDBClusterEndpointsRequest()
      .withDBClusterIdentifier(clusterArn.getResource().getResource());

    DescribeDBClusterEndpointsResult dbClusterEndpoints = rdsClient.describeDBClusterEndpoints(req);

    return dbClusterEndpoints.getDBClusterEndpoints().stream().filter(e -> type.toString().equals(e.getEndpointType()))
      .filter(ep -> ep.getStatus().equals("available"))
      .findFirst()
      .map(DBClusterEndpoint::getEndpoint);
  }

  static void printPersistenceConfig(GlobalPersistenceClusterEndpointType type, HikariConfig hikariConfig) {
    LOGGER.info("************ Persistence configurations for type {} ************", type);
    LOGGER.info("""
        
        Endpoint Type: {}
        JDBC URL: {}
        Pool Size: {}
        
      """, type, hikariConfig.getJdbcUrl(), hikariConfig.getMaximumPoolSize());
  }
}
