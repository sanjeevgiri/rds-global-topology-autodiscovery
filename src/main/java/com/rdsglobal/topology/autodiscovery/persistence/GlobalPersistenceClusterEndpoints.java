package com.rdsglobal.topology.autodiscovery.persistence;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GlobalPersistenceClusterEndpoints {
  private String globalClusterIdentifier;
  private String writerJdbcUrl;
  private String readerJdbcUrl;
}
