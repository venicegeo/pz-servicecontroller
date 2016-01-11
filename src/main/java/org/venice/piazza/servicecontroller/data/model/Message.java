package org.venice.piazza.servicecontroller.data.model;

public class Message {
	
	public static final String UPPER="UPPER";
	public static final String LOWER="LOWER";
	String theString;
	String conversionType;

	public String getConversionType() {
		return conversionType;
	}

	public void setConversionType(String conversionType) {
		this.conversionType = conversionType;
	}

	public String gettheString() {
		return theString;
	}

	public void settheString(String theString) {
		this.theString = theString;
	}
	

}
