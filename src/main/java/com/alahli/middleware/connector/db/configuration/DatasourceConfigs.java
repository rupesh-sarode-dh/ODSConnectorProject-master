package com.alahli.middleware.connector.db.configuration;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

//import oracle.ucp.jdbc.PoolDataSourceImpl;;

//@Configuration
public class DatasourceConfigs {

	/*
	 * @Bean("odsdbproperties")
	 * 
	 * @ConfigurationProperties("connector.database.datasource.odsdb") public
	 * DataSourceProperties bancsdaydatasourceproperties() { return new
	 * DataSourceProperties(); }
	 * 
	 * @Bean(name = "odsdb")
	 * 
	 * @Primary
	 * 
	 * @ConfigurationProperties("connector.database.datasource.odsdb.configuration")
	 * public PoolDataSourceImpl bancsDayDataSource(
	 * 
	 * @Qualifier("odsdbproperties") DataSourceProperties
	 * bancsdaydatasourceproperties) { return
	 * bancsdaydatasourceproperties.initializeDataSourceBuilder().type(oracle.ucp.
	 * jdbc.PoolDataSourceImpl.class) .build();
	 * 
	 * }
	 */

}