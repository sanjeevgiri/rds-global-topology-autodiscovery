package com.rdsglobal.topology.autodiscovery.persistence;


import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy;
import org.hibernate.boot.model.naming.ImplicitNamingStrategyComponentPathImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableJpaRepositories(basePackages = "com.rdsglobal.topology.autodiscovery", entityManagerFactoryRef = "writerEntityManager", transactionManagerRef = "writerTransactionManager")
public class GlobalPersistenceClusterConfig {
  @Bean
  @ConfigurationProperties(prefix = "autodiscovery.datasource")
  public GlobalPersistenceClusterProperties globalClusterProperties() {
    return new GlobalPersistenceClusterProperties();
  }

  @Bean
  public AmazonRDS rdsGlobalClient() {
    return AmazonRDSClientBuilder.standard().build();
  }

  @Bean
  public GlobalPersistenceClusterEndpoints globalClusterEndpoints() {
    return GlobalPersistenceClusterUtil
      .globalClusterEndpoints(rdsGlobalClient(), globalClusterProperties());
  }

  @Bean
  @Primary
  @ConfigurationProperties(prefix = "autodiscovery.datasource.writer")
  public HikariConfig writerHikariConfig(GlobalPersistenceClusterEndpoints globalClusterEps) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(globalClusterEps.getWriterJdbcUrl());
    return config;
  }

  @Bean
  @Primary
  public DataSource writerDatasource(HikariConfig writerHikariConfig) {
    GlobalPersistenceClusterUtil.printPersistenceConfig(GlobalPersistenceClusterEndpointType.WRITER, writerHikariConfig);
    return new HikariDataSource(writerHikariConfig);
  }

  @Bean
  @Primary
  public LocalContainerEntityManagerFactoryBean writerEntityManager(DataSource writerDatasource) {
    return entityManager(writerDatasource, true, "writer");
  }

  @Primary
  @Bean("writerTransactionManager")
  public PlatformTransactionManager writerTransactionManager(LocalContainerEntityManagerFactoryBean writerEm) {
    return transactionManager(writerEm);
  }

  @Bean("readerHirakiConfig")
  @ConfigurationProperties(prefix = "autodiscovery.datasource.reader")
  public HikariConfig readerHirakiConfig(GlobalPersistenceClusterEndpoints globalClusterEps) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(globalClusterEps.getReaderJdbcUrl());
    return config;
  }

  @Bean("readerDatasource")
  public DataSource readerDatasource(@Qualifier("readerHirakiConfig") HikariConfig readerHirakiConfig) {
    GlobalPersistenceClusterUtil.printPersistenceConfig(GlobalPersistenceClusterEndpointType.READER, readerHirakiConfig);
    return new HikariDataSource(readerHirakiConfig);
  }

  @Bean("readerEm")
  public LocalContainerEntityManagerFactoryBean
  readerEntityManager(@Qualifier("readerDatasource") DataSource readerDatasource) {
    return entityManager(readerDatasource, false, "reader");
  }

  @Bean("readerTransactionManager")
  public PlatformTransactionManager
  readerTransactionManager(@Qualifier("readerEm") LocalContainerEntityManagerFactoryBean readerEm) {
    return transactionManager(readerEm);
  }

  private PlatformTransactionManager transactionManager(LocalContainerEntityManagerFactoryBean em) {
    JpaTransactionManager transactionManager = new JpaTransactionManager();
    transactionManager.setEntityManagerFactory(em.getObject());
    return transactionManager;
  }

  private LocalContainerEntityManagerFactoryBean entityManager(DataSource ds, boolean writer, String puName) {
    LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
    em.setDataSource(ds);
    em.setPackagesToScan("com.rdsglobal.topology.autodiscovery");
    em.setPersistenceUnitName(puName);
    em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
    em.setJpaPropertyMap(jpaProperties(writer));
    return em;
  }

  private Map<String, Object> jpaProperties(boolean writer) {
    Map<String, Object> props = new HashMap<>();
    props.put("hibernate.physical_naming_strategy", CamelCaseToUnderscoresNamingStrategy.class.getName());
    props.put("hibernate.implicit_naming_strategy", ImplicitNamingStrategyComponentPathImpl.class.getName());
    if (writer) {
      props.put("hibernate.hbm2ddl.auto", "update"); // Do not use this for cases other than POC and research
    }
    return props;
  }
}

