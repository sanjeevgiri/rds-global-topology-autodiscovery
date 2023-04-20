package com.rdsglobal.topology.autodiscovery.persistence;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GlobalPersistenceClusterProperties {
  private String clientAppRegion;
  private String globalClusterId;
  private String globalMemberlessClusterPrefferedWriter;
  private String name;
  private String port;
}
