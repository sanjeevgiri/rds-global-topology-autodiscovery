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
import com.amazonaws.services.rds.model.GlobalCluster;
import com.amazonaws.services.rds.model.GlobalClusterMember;
import com.zaxxer.hikari.HikariConfig;
import io.vavr.control.Try;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
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

    String writerEndpoint = clusterEndpoint(writerRdsClient, writer, GlobalPersistenceClusterEndpointType.WRITER);
    String readerEndpoint = clusterEndpoint(readerRdsClient, reader, GlobalPersistenceClusterEndpointType.READER);

    return GlobalPersistenceClusterEndpoints.builder()
      .globalClusterIdentifier(props.getGlobalClusterId())
      .readerJdbcUrl(jdbcUrl(readerEndpoint, props.getPort(), props.getName()))
      .writerJdbcUrl(jdbcUrl(writerEndpoint, props.getPort(), props.getName()))
      .build();
  }

  public static GlobalPersistenceClusterEndpoints globalClusterEndpoints(
    AmazonRDS rdsGlobalClient, GlobalPersistenceClusterProperties props
  ) {
    GlobalCluster globalCluster = globalCluster(rdsGlobalClient, props.getGlobalClusterId());
    if(CollectionUtils.isEmpty(globalCluster.getGlobalClusterMembers())) {
      LOGGER.error("Detected memberless global cluster {}", props.getGlobalClusterId());
      DBCluster preferredWriter = preferredWriterDbCluster(props);
      globalCluster = reconfiguredGlobalMemberlessClusterWithPreferredWriter(rdsGlobalClient, props, preferredWriter);
    }
    return  globalClusterEndpoints(globalCluster, props);
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
    rdsGlobalClient
      .deleteGlobalCluster(new DeleteGlobalClusterRequest().withGlobalClusterIdentifier(props.getGlobalClusterId()));

    return rdsGlobalClient.createGlobalCluster(new CreateGlobalClusterRequest()
      .withGlobalClusterIdentifier(props.getGlobalClusterId())
      .withSourceDBClusterIdentifier(dbCluster.getDBClusterArn())
    );
  }

  public static GlobalCluster globalCluster(AmazonRDS amazonRdsGlobalClient, String globalClusterIdentifier) {
    DescribeGlobalClustersRequest descGlobalClusterReq = new DescribeGlobalClustersRequest();
    descGlobalClusterReq.setGlobalClusterIdentifier(globalClusterIdentifier);

    return amazonRdsGlobalClient
      .describeGlobalClusters(descGlobalClusterReq)
      .getGlobalClusters()
      .stream()
      .findFirst()
      .orElseThrow(() -> new RuntimeException("Unable to find global cluster " + globalClusterIdentifier));
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

  private static String clusterEndpoint(
    AmazonRDS rdsClient, GlobalClusterMember member, GlobalPersistenceClusterEndpointType type
  ) {
    Arn clusterArn = Arn.fromString(member.getDBClusterArn());

    DescribeDBClusterEndpointsRequest req = new DescribeDBClusterEndpointsRequest()
      .withDBClusterIdentifier(clusterArn.getResource().getResource());

    DescribeDBClusterEndpointsResult dbClusterEndpoints = rdsClient.describeDBClusterEndpoints(req);

    return dbClusterEndpoints.getDBClusterEndpoints().stream().filter(e -> type.toString().equals(e.getEndpointType()))
      .findFirst()
      .map(DBClusterEndpoint::getEndpoint)
      .orElseThrow(() -> new RuntimeException("Unable to find endpoint for member " + member.getDBClusterArn()));
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
