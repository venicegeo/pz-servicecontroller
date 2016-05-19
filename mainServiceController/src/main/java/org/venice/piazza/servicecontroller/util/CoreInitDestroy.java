/*******************************************************************************
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.venice.piazza.servicecontroller.util;

/**
 * Bean which is responsible for registering with the pz-discover service and initializing properties and 
 * other resources.
 * @author mlynum
 * @version 1.0
 */

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PreDestroy;

import model.resource.CoreResource;
import model.resource.DBCoreResource;
import model.resource.KafkaCoreResource;
import model.resource.RegisterService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

public class CoreInitDestroy implements InitializingBean {
	
	 final static Logger LOGGER = LoggerFactory.getLogger(CoreInitDestroy.class);
	 
	 @Autowired
	 private CoreServiceProperties coreServiceProperties;
	 private String discoverService;
	 private String appName;
	 private String url;
	 private String host;
	 private String discoverAPI;

	/**
	 * Constructor
	 */
	 public CoreInitDestroy() {
		 LOGGER.info("Constructor called");

			 
	 }
	 @PreDestroy
	 /** 
	  * Clean up resources and de-register...
	  * @throws Exception
	  */
	 public void cleanUp() throws Exception {
		  LOGGER.info("Cleaning Up");
	  
	 }

	 @Override
	 /** 
	  * Set the properties by calling the pz-discover service if enabled
	  */
	 public void afterPropertiesSet() throws Exception {
	
		
	 }
	 
	
	 
	 
}