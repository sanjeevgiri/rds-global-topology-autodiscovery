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
import java.util.List;
import java.util.Optional;

public final class GlobalPersistenceClusterUtil {
  private GlobalPersistenceClusterUtil() {
  }

  static GlobalPersistenceClusterEndpoints globalClusterEndpoints(
    AmazonRDS rdsGlobalClient, AmazonRDS rdsRegionalClient, GlobalPersistenceClusterProperties props
  ) {
    GlobalCluster globalCluster = globalCluster(rdsGlobalClient, props.getGlobalClusterId());
    GlobalClusterMember writer = writerCluster(globalCluster.getGlobalClusterMembers());
    GlobalClusterMember reader = readerCluster(globalCluster.getGlobalClusterMembers(), props.getClientAppRegion());
    String writerEndpoint = clusterEndpoint(rdsRegionalClient, writer, GlobalPersistenceClusterEndpointType.WRITER);
    String readerEndpoint = clusterEndpoint(rdsRegionalClient, reader, GlobalPersistenceClusterEndpointType.READER)

    GlobalPersistenceClusterEndpoints clusterTopology = new GlobalPersistenceClusterEndpoints();

    clusterTopology.setWriterJdbcUrl(jdbcUrl(writerEndpoint, props.getName(), props.getPort()));
    return null;
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
    AmazonRDS rdsRegionalClient, GlobalClusterMember member, GlobalPersistenceClusterEndpointType type
  ) {
    Arn clusterArn = Arn.fromString(member.getDBClusterArn());
    DescribeDBClusterEndpointsRequest req = new DescribeDBClusterEndpointsRequest();
    req.setDBClusterEndpointIdentifier(clusterArn.getResourceAsString());

    AmazonRDS rds = AmazonRDSClientBuilder.standard().withRegion(clusterArn.getRegion()).build();
    DescribeDBClusterEndpointsResult dbClusterEndpoints = rds.describeDBClusterEndpoints(req);

    return dbClusterEndpoints.getDBClusterEndpoints().stream().filter(e -> type.toString().equals(e.getEndpointType()))
      .findFirst()
      .map(DBClusterEndpoint::getEndpoint)
      .orElseThrow(() -> new RuntimeException("Unable to find endpoint for member " + member.getDBClusterArn()));
  }
}
