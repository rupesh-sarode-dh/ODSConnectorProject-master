
package com.alahli.middleware.connector.db.model;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude;

@Component
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FaultType {

	private String type;
	private String number;
	private String description;
	private String retryAfter;
	private String system;
	private String nativeError;
	private String nativeDescription;

	public String getType() {
		return type;
	}

	public void setType(String value) {
		this.type = value;
	}

	public String getNumber() {
		return number;
	}

	public void setNumber(String number2) {
		this.number = number2;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String value) {
		this.description = value;
	}

	public String getRetryAfter() {
		return retryAfter;
	}

	public void setRetryAfter(String value) {
		this.retryAfter = value;
	}

	public String getSystem() {
		return system;
	}

	public void setSystem(String value) {
		this.system = value;
	}

	public String getNativeError() {
		return nativeError;
	}

	public void setNativeError(String value) {
		this.nativeError = value;
	}

	public String getNativeDescription() {
		return nativeDescription;
	}

	public void setNativeDescription(String value) {
		this.nativeDescription = value;
	}

}
