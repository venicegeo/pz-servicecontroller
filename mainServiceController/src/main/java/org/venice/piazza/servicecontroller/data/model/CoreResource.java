package org.venice.piazza.servicecontroller.data.model;



public class CoreResource {
	
	private String type;
	private String address;
	private String host;
	private int port;
	
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String host) {
		this.address = host;
	}

}