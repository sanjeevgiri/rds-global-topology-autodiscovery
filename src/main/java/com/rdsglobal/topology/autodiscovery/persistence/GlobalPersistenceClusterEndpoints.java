package com.rdsglobal.topology.autodiscovery.persistence;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class GlobalPersistenceClusterEndpoints {
  private String globalClusterIdentifier;
  private String writerJdbcUrl;
  private String readerJdbcUrl;
}
