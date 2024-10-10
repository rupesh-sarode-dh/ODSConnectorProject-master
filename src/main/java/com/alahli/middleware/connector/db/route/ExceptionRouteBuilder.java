package com.alahli.middleware.connector.db.route;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;

public abstract class ExceptionRouteBuilder extends RouteBuilder {

	public void configureException() throws Exception {

	
		//Handles all Exception
        onException(Exception.class)
		 .handled(true)
	//	 .setHeader(Exchange.HTTP_RESPONSE_CODE,constant(400))
		// .log("General Exception "+ "${exception}")
		  .to("bean:com.alahli.middleware.connector.db.bean.ExceptionHandler?method=handleGenericException("+"${exception}"+")");
		//  .marshal().json(JsonLibrary.Jackson);;

		
		
	
	}
}
