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
  public HikariConfig writerHikariConfig() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(globalClusterEndpoints().getWriterJdbcUrl());
    return new HikariConfig();
  }

  @Bean
  @Primary
  public DataSource writerDatasource() {
    return new HikariDataSource(writerHikariConfig());
  }

  @Bean
  @Primary
  public LocalContainerEntityManagerFactoryBean writerEntityManager() {
    return entityManager(writerDatasource(), "writer");
  }

  @Primary
  @Bean("writerTransactionManager")
  public PlatformTransactionManager writerTransactionManager() {
    return transactionManager(writerEntityManager());
  }

  @Bean
  @ConfigurationProperties(prefix = "autodiscovery.datasource.reader")
  public HikariConfig readerHirakiConfig() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(globalClusterEndpoints().getReaderJdbcUrl());
    return new HikariConfig();
  }


  @Bean
  public DataSource readerDatasource() {
    return new HikariDataSource(readerHirakiConfig());
  }

  @Bean
  public LocalContainerEntityManagerFactoryBean readerEntityManager() {
    return entityManager(readerDatasource(), "reader");
  }

  @Bean("readerTransactionManager")
  public PlatformTransactionManager readerTransactionManager() {
    return transactionManager(readerEntityManager());
  }

  private PlatformTransactionManager transactionManager(LocalContainerEntityManagerFactoryBean em) {
    JpaTransactionManager transactionManager = new JpaTransactionManager();
    transactionManager.setEntityManagerFactory(em.getObject());
    return transactionManager;
  }

  private LocalContainerEntityManagerFactoryBean entityManager(DataSource ds, String persistenceUnitName) {
    LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
    em.setDataSource(ds);
    em.setPackagesToScan("com.rdsglobal.topology.autodiscovery");
    em.setPersistenceUnitName(persistenceUnitName);
    em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
    em.setJpaPropertyMap(jpaProperties());
    return em;
  }

  private Map<String, Object> jpaProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put("hibernate.physical_naming_strategy", CamelCaseToUnderscoresNamingStrategy.class.getName());
    props.put("hibernate.implicit_naming_strategy", ImplicitNamingStrategyComponentPathImpl.class.getName());
    props.put("hibernate.hbm2ddl.auto", "update"); // Do not use this for uses other than POC and research
    return props;
  }
}

