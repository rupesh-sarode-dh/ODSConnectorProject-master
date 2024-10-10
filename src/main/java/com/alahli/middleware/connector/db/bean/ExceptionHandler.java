package com.alahli.middleware.connector.db.bean;

import java.sql.SQLException;

import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.alahli.middleware.connector.db.model.FaultType;

@Component
public class ExceptionHandler extends Exception {

	@Autowired
	FaultType faultType;

	@Autowired
	Environment environment;

	// Error values related to database timeout
	@Value("${connector.database.errors.databaseError.timeout.nativeError}")
	private String dbTimeoutNativeError;
	@Value("${connector.database.errors.databaseError.timeout.errorNumber}")
	private String dbTimeoutErrorNumber;
	@Value("${connector.database.errors.databaseError.timeout.errorDescription}")
	private String dbTimeoutErrorDescription;

	// Error values related to database general exception
	@Value("${connector.database.errors.databaseError.general.nativeError}")
	private String dbGeneralNativeError;
	@Value("${connector.database.errors.databaseError.general.errorNumber}")
	private String dbGeneralErrorNumber;
	@Value("${connector.database.errors.databaseError.general.errorDescription}")
	private String dbGeneralErrorDescription;

	// Error values related to application timeout exception
	@Value("${connector.database.errors.applicationError.timeout.nativeError}")
	private String appTimeoutNativeError;
	@Value("${connector.database.errors.applicationError.timeout.errorNumber}")
	private String appTimeoutErrorNumber;
	@Value("${connector.database.errors.applicationError.timeout.errorDescription}")
	private String appTimeoutErrorDescription;

	// Error values related to application general exception
	@Value("${connector.database.errors.applicationError.general.nativeError}")
	private String appGeneralNativeError;
	@Value("${connector.database.errors.applicationError.general.errorNumber}")
	private String appGeneralErrorNumber;
	@Value("${connector.database.errors.applicationError.general.errorDescription}")
	private String appGeneralErrorDescription;

	// System name based on database or application
	@Value("${connector.database.errors.databaseError.system}")
	private String dbSystem;
	@Value("${connector.database.errors.applicationError.system}")
	private String appSystem;

	static Logger logger = LoggerFactory.getLogger(ExceptionHandler.class);

	public FaultType handleSQLException(SQLException exception, Exchange exchange) {
		logger.error("SQL Exception {} ", exception.getMessage());

		faultType.setSystem(dbSystem);
		faultType.setNativeDescription(exception.getMessage());

		// Timeout Error
		if (exception.getMessage().contains("timeout")) {
 
			faultType.setNumber(dbTimeoutErrorNumber);
			faultType.setNativeError(dbTimeoutNativeError);
			faultType.setDescription(dbTimeoutErrorDescription);

		}
		// Application Error
		else {
			faultType.setNumber(dbGeneralErrorNumber);
			faultType.setDescription(dbGeneralErrorDescription);
			if (exception.getSQLState() != null) {
				faultType.setNativeError(exception.getSQLState());
			} else {
				faultType.setNativeError(dbGeneralNativeError);
			}

		}

		return faultType;
	}

	public FaultType handleGeneralException(Exception exception, Exchange exchange) {
		logger.error("Exception {}",exception.getMessage());
         
		faultType.setSystem(appSystem);
		faultType.setNativeDescription(exception.getMessage());

		// Timeout Error
		if (exception.getMessage().contains("timeout")) {

			faultType.setNumber(appTimeoutErrorNumber);
			faultType.setDescription(appTimeoutErrorDescription);
			faultType.setNativeError(appTimeoutNativeError);
		}
		// Application Error
		else {
			faultType.setNumber(appGeneralErrorNumber);
			faultType.setDescription(appGeneralErrorDescription);
			faultType.setNativeError(appGeneralNativeError);

		}

		return faultType;
	}
	
	
	public String handleGenericException(Exception exception, Exchange exchange) {
		logger.error("Exception ",exception);
       
		if (exception.getMessage().contains("timeout")) {
			
			exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 408);
			
		}
		
		else  {
			exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
			
		}
		

		return exception!=null && exception.getMessage()!=null?exception.getMessage():"DB Exception";
	}

}
