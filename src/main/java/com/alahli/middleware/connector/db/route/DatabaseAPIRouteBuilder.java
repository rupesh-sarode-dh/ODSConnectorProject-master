package com.alahli.middleware.connector.db.route;

import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.stereotype.Component;

@Component
public class DatabaseAPIRouteBuilder extends ExceptionRouteBuilder {

	@Override
	public void configure() throws Exception {
		super.configureException();
		restConfiguration()
		    .bindingMode(RestBindingMode.json);

		rest("{{connector.database.api.basepath}}")
		 .post("{version}/{operation}")
		  .consumes("application/json")
		   .produces("application/json")
		    .to("direct:invokeOperation");
		
		rest("{{connector.database.api.basepath}}")
		 .get("{version}/{operation}?")
		  .consumes("application/json")
		   .produces("application/json")
		    .to("direct:invokeOperation");
		
		rest("{{connector.database.api.basepath}}")
		 .delete("{version}/{operation}")
		  .consumes("application/json")
		   .produces("application/json")
		    .to("direct:invokeOperation");
		
		rest("{{connector.database.api.basepath}}")
		 .put("{version}/{operation}")
		  .consumes("application/json")
		   .produces("application/json")
		    .to("direct:invokeOperation");

		from("direct:invokeOperation")
		  //.log("invokeOperation")
		  .routeId("invoke-db-Connector-api-route")
		   .routeDescription("Invokes the Bean from the config")
			 .to("bean:com.alahli.middleware.connector.db.utility.Utility?method=fetchBeanNameByOperation")
			  .toD("bean:" + "${headers.beanpath}" + "?method=" + "${headers.operation}");

	}

}
