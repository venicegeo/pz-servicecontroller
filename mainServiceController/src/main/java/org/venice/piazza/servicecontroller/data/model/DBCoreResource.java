package org.venice.piazza.servicecontroller.data.model;


import com.fasterxml.jackson.annotation.JsonProperty;

public class DBCoreResource {
	
	private String type;
	@JsonProperty("db-uri")
	private String host;
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}

}
