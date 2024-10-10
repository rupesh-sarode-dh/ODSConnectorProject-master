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
public class GetCifByWatheeqTest {


	@Autowired
	CamelContext camelContext;

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	ProducerTemplate producerTemplate;

	@Autowired
	ApplicationContext applicationContext;
	
	@Test
	public void getCifByWatheeq_Success() throws Exception {

		String getCifByWatheeqRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetCifByWatheeq/GetCifByWatheeq_Request.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		
		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(getCifByWatheeqRequest, mapType);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v1");
		headers.put("operation","GetCifByWatheeq");
		headers.put("CamelHttpMethod","POST");
		JsonNode getCifByWatheeqResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, JsonNode.class);
		System.out.println("getCifByWatheeqResponse:" + getCifByWatheeqResponse.get("CifByWatheeqResponse").get("success"));

		Assertions.assertTrue(null != getCifByWatheeqResponse.get("CifByWatheeqResponse").get("success"));

	}
	
	@Test
	public void getCifByWatheeq_ERR_msgNotEqualToSuccess() throws Exception {

		String getCifByWatheeqRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetCifByWatheeq/GetCifByWatheeq_ERR_msgNotEqualToSuccess.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(getCifByWatheeqRequest, mapType);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v1");
		headers.put("operation","GetCifByWatheeq");
		headers.put("CamelHttpMethod","POST");
		String getCifByWatheeqResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, String.class);
		System.out.println("getCifByWatheeqResponse:" + getCifByWatheeqResponse);

		Assertions.assertTrue(getCifByWatheeqResponse.contains("GetCifByWatheeq Service Error"));

	}
}
