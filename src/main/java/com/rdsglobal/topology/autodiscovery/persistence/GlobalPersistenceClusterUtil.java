package com.rdsglobal.topology.autodiscovery.persistence;

import com.amazonaws.arn.Arn;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBClusterEndpoint;
import com.amazonaws.services.rds.model.DescribeDBClusterEndpointsRequest;
import com.amazonaws.services.rds.model.DescribeDBClusterEndpointsResult;
import com.amazonaws.services.rds.model.DescribeGlobalClustersRequest;
import com.amazonaws.services.rds.model.GlobalCluster;
import com.amazonaws.services.rds.model.GlobalClusterMember;
import com.zaxxer.hikari.HikariConfig;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GlobalPersistenceClusterUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPersistenceClusterUtil.class.getName());

  private GlobalPersistenceClusterUtil() {
  }

  static GlobalPersistenceClusterEndpoints globalClusterEndpoints(
    AmazonRDS rdsGlobalClient, GlobalPersistenceClusterProperties props
  ) {
    GlobalCluster globalCluster = globalCluster(rdsGlobalClient, props.getGlobalClusterId());
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

    GlobalPersistenceClusterEndpoints clusterTopology = new GlobalPersistenceClusterEndpoints();

    clusterTopology.setWriterJdbcUrl(jdbcUrl(writerEndpoint, props.getPort(), props.getName()));
    clusterTopology.setReaderJdbcUrl(jdbcUrl(readerEndpoint, props.getPort(), props.getName()));
    return clusterTopology;
  }

  private static String jdbcUrl(String endpoint, String port, String database) {
    return String.format("jdbc:aws-wrapper:postgresql://%s:%s/%s", endpoint, port, database);
  }

  private static GlobalCluster globalCluster(AmazonRDS amazonRdsGlobalClient, String globalClusterIdentifier) {
    DescribeGlobalClustersRequest descGlobalClusterReq = new DescribeGlobalClustersRequest();
    descGlobalClusterReq.setGlobalClusterIdentifier(globalClusterIdentifier);

    return amazonRdsGlobalClient
      .describeGlobalClusters(descGlobalClusterReq)
      .getGlobalClusters()
      .stream()
      .findFirst()
      .orElseThrow(() -> new RuntimeException("Unable to find global cluster " + globalClusterIdentifier));
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
