package com.alahli.middleware.connector.db.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.web.WebAppConfiguration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

@CamelSpringBootTest
@SpringBootApplication
@WebAppConfiguration

@UseAdviceWith
@ImportResource({"classpath:spring/camel-context.xml"})
@Configuration
@PropertySource("classpath:application-test.properties")
@ComponentScan("com.alahli.middleware.connector.db.*")
public class GetCustomerDetailsTest {


	@Autowired
	CamelContext camelContext;

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	ProducerTemplate producerTemplate;

	@Autowired
	ApplicationContext applicationContext;
	
	@Test
	public void getCustomerDetails_Success() throws Exception {

		String getCustomerDetailsRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetCustomerDetails/GetCustomerDetails_Request.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		
		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(getCustomerDetailsRequest, mapType);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v1");
		headers.put("operation","GetCustomerDetails");
		headers.put("CamelHttpMethod","POST");
		JsonNode getCustomerDetailsResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, JsonNode.class);
		System.out.println("getCustomerDetailsResponse:" + getCustomerDetailsResponse.get("CustomerDetailsResponse"));

		Assertions.assertTrue(null != getCustomerDetailsResponse.get("CustomerDetailsResponse"));

	}
}
