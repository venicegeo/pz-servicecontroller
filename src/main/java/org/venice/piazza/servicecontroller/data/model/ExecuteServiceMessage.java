package org.venice.piazza.servicecontroller.data.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExecuteServiceMessage {
	
	@JsonProperty("dataInputs")
	public Map<String, String> dataInputs;	
	public String dataInput;
	public String resourceId;

}
