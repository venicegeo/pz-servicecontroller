package org.venice.piazza.servicecontroller.data.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
	 * Serves as the data model for the parameters associated registering with pz-discover
	 * 
	 * @author mlynum
	 * @date Dec 16, 2015
	 *
	 **/
public class RegisterService {
		private String name;
		@JsonProperty("data")
		private Map<String, String> data;
		
		
		public Map<String, String> getData() {
			return data;
		}
		public void setData(Map<String, String> data) {
			this.data = data;
		}
		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}
		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}
		

}
