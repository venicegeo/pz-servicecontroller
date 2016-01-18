package org.venice.piazza.servicecontroller.data.model;

/* Copyright 2015, RadiantBlue Technologies, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */


/**
 * SecAttributes
 * 
 * @author mlynum
 * @date Dec 16, 2015
 *
 **/
public class SecAttributes {
	
	private Boolean clientCertRequired;
	private Boolean credentialsRequired;
	private Boolean preAuthRequired;
	
	/**
	 * @return the clientCertRequired
	 */
	public Boolean getClientCertRequired() {
		return clientCertRequired;
	}
	/**
	 * @param clientCertRequired the clientCertRequired to set
	 */
	public void setClientCertRequired(Boolean clientCertRequired) {
		this.clientCertRequired = clientCertRequired;
	}
	/**
	 * @return the credentialsRequired
	 */
	public Boolean getCredentialsRequired() {
		return credentialsRequired;
	}
	/**
	 * @param credentialsRequired the credentialsRequired to set
	 */
	public void setCredentialsRequired(Boolean credentialsRequired) {
		this.credentialsRequired = credentialsRequired;
	}
	/**
	 * @return the preAuthRequired
	 */
	public Boolean getPreAuthRequired() {
		return preAuthRequired;
	}
	/**
	 * @param preAuthRequired the preAuthRequired to set
	 */
	public void setPreAuthRequired(Boolean preAuthRequired) {
		this.preAuthRequired = preAuthRequired;
	}

}