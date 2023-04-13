package com.rdsglobal.topology.autodiscovery.persistence;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class GlobalPersistenceClusterEndpoints {
  private String globalClusterIdentifier;
  private String writerJdbcUrl;
  private String readerJdbcUrl;
}
