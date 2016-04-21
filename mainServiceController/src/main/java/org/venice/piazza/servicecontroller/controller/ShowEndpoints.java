package org.venice.piazza.servicecontroller.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.AbstractEndpoint;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.stereotype.Component;

@Component


public class ShowEndpoints extends  AbstractEndpoint<List<Endpoint>>{
	
	private List<Endpoint> endpoints;
	
	@Autowired
	// Constructor used to show endpoints
	public ShowEndpoints(List<Endpoint>endpoints) {
		super ("showEndpoints");
		this.endpoints = endpoints;
	}
	/**
	 * Returns a list of end points
	 * @return list of end points
	 */
	public List<Endpoint> invoke() {
		return this.endpoints;
	}

}
