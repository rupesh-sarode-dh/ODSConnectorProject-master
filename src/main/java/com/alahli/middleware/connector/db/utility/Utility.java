package com.alahli.middleware.connector.db.utility;

import org.apache.camel.Exchange;
//import org.apache.camel.language.Simple;
import org.apache.camel.language.simple.Simple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
@Component
public class Utility {
	@Autowired
	Environment environment;

	static Logger odsdbLogger = LoggerFactory.getLogger(Utility.class);

	@Value("${connector.database.beans.implementationPath}")
	String beanImplPath;

	public void fetchBeanNameByOperation(@Simple("${header.version}") String version,
			@Simple("${header.operation}") String operationName, @Simple("${header.CamelHttpMethod}") String httpMethod,
			Exchange exchange) throws Exception {
		
        String cHttpMehtod=environment.getProperty(beanImplPath + "." + operationName + "." + "httpmethod");
		
        if (httpMethod == null || httpMethod.equals("")
		||cHttpMehtod==null || cHttpMehtod.equals("")|| !cHttpMehtod.equals(httpMethod) ) {
			
			throw new Exception("HTTP method "+httpMethod+" for API " + version + "/" + operationName + "is not available ");
		}

		/*
		 * Value of version is null, empty and s start with any character other than 'v'
		 * then throw error
		 */
		if (null == version || version.trim().equals("") || !version.startsWith("v")) {
			throw new Exception("API version " + version + " is not available");
		}

		// Value of operation is null and empty then throw error
		if (null == operationName || operationName.trim().equals("")) {
			throw new Exception("API " + version + "/" + operationName + "is not available ");
		}

		/*
		 * Fetch bean class-path value based on the operation and version from property
		 * file
		 */
		// System.out.println(beanImplPath + "." + operationName + "." + version);
		String beanClassPath = environment.getProperty(beanImplPath + "." + operationName + "." + version);

		// Value of operation is null and empty then throw error
		if (null == beanClassPath || beanClassPath.trim().equals("")) {
			throw new Exception("No matching implementation Found for " + "API " + version + "/" + operationName);
		}

		// Adding a new header bean-path in the exchange
		if (exchange != null) {
			exchange.getIn().getHeaders().put("beanpath", beanClassPath);
		}
		// System.out.println("Found "+beanClassPath);

	}

}
