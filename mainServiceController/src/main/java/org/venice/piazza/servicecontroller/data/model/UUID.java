package org.venice.piazza.servicecontroller.data.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UUID {
	@JsonProperty("data")
	 public List<String> data;

	public List<String> getData() {
		return data;
	}

	public void setData(List<String> data) {
		this.data = data;
	}
}
