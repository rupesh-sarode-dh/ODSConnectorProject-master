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
public class GetCustomerOneViewTest {


	@Autowired
	CamelContext camelContext;

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	ProducerTemplate producerTemplate;

	@Autowired
	ApplicationContext applicationContext;
	
	@Test
	public void GetCustomerOneViewAll_Success() throws Exception {

		String GetCustomerOneViewRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetCustomerOneView/CustomerOneView_RequestAll.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		
		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(GetCustomerOneViewRequest, mapType);


		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v1");
		headers.put("operation","GetCustomerOneView");
		headers.put("CamelHttpMethod","POST");
		JsonNode GetCustomerOneViewResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, JsonNode.class);
		System.out.println("GetCustomerOneViewResponse:" + GetCustomerOneViewResponse.get("CustomerOneViewResponse"));

		Assertions.assertTrue(null != GetCustomerOneViewResponse.get("CustomerOneViewResponse"));

	}
	
	@Test
	public void GetCustomerOneViewOtherThanAll_Success() throws Exception {

		String GetCustomerOneViewRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetCustomerOneView/CustomerOneView_RequestOtherThanAll.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		
		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(GetCustomerOneViewRequest, mapType);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v1");
		headers.put("operation","GetCustomerOneView");
		headers.put("CamelHttpMethod","POST");
		JsonNode GetCustomerOneViewResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, JsonNode.class);
		System.out.println("GetCustomerOneViewResponse:" + GetCustomerOneViewResponse.get("CustomerOneViewResponse"));

		Assertions.assertTrue(null != GetCustomerOneViewResponse.get("CustomerOneViewResponse"));

	}
}
