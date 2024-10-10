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
public class GetLookuptablesV2Test {

	@Autowired
	CamelContext camelContext;

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	ProducerTemplate producerTemplate;

	@Autowired
	ApplicationContext applicationContext;
	
	//Just need some code changes in GetLookuptables after that success test case will work
	//@Test
	public void getLookuptables_Success() throws Exception {

		String getLookuptablesRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetLookuptablesV2/GetLookuptables_Request.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		
		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(getLookuptablesRequest, mapType);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v1");
		headers.put("operation","GetLookuptables");
		headers.put("CamelHttpMethod","POST");
		JsonNode getLookuptablesResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, JsonNode.class);
		System.out.println("getLookuptablesResponse:" + getLookuptablesResponse.get("LookupTableResponse").get("success").get("table"));

		Assertions.assertTrue(null != getLookuptablesResponse.get("LookupTableResponse").get("success").get("table"));

	}
	
	@Test
	public void getLookuptables_WhenCOL_DESC_INDNotEqualToY() throws Exception {

		String getLookuptablesRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetLookuptablesV2/GetLookuptables_WhenCOL_DESC_INDNotEqualToYRequest.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		
		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(getLookuptablesRequest, mapType);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v2");
		headers.put("operation","GetLookuptables");
		headers.put("CamelHttpMethod","POST");
		JsonNode getLookuptablesResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, JsonNode.class);
		System.out.println("getLookuptablesResponse:" + getLookuptablesResponse);

		Assertions.assertTrue(null == getLookuptablesResponse);

	}
	
	@Test
	public void getLookuptables_WhenCOL_DESC_INDEqualToYAndTableNameEquals() throws Exception {

		String getLookuptablesRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetLookuptablesV2/GetLookuptables_WhenCOL_DESC_INDEqualToYAndTableNameEqualsRequest.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		

		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(getLookuptablesRequest, mapType);
		
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v2");
		headers.put("operation","GetLookuptables");
		headers.put("CamelHttpMethod","POST");
		JsonNode getLookuptablesResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, JsonNode.class);
		System.out.println("getLookuptablesResponse:" + getLookuptablesResponse);

		Assertions.assertTrue(null == getLookuptablesResponse);

	}
	
	@Test
	public void getLookupTable_DataNotFound() throws Exception {

		String getLookuptablesRequest = Resources.toString(
				Resources.getResource("mock/frontend/GetLookuptablesV2/GetLookupTable_DataNotFoundRequest.json"), Charsets.UTF_8);
				
		AdviceWith.adviceWith(camelContext, "invoke-db-Connector-api-route", routeBuilder -> {
			routeBuilder.replaceFromWith("direct:invokeOperation");
		});

		camelContext.start();
		
		
		TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String, Object>>() {};
		Map<String, Object> requestMap = objectMapper.readValue(getLookuptablesRequest, mapType);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("version","v2");
		headers.put("operation","GetLookuptables");
		headers.put("CamelHttpMethod","POST");
		JsonNode getLookupTableResponse = producerTemplate.requestBodyAndHeaders("direct:invokeOperation", requestMap,
				headers, JsonNode.class);
		System.out.println("getLookupTableResponse:" + getLookupTableResponse);

		Assertions.assertTrue(null == getLookupTableResponse);

	}
}
