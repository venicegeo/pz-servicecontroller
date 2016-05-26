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
package org.venice.piazza.servicecontroller.messaging.handlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author mlynum
 * @version 1.0
 */
import model.job.PiazzaJobType;
import model.job.type.DescribeServiceMetadataJob;
import model.service.metadata.Service;
import util.PiazzaLogger;

public class DescribeServiceHandler implements PiazzaJobHandler { 
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DescribeServiceHandler.class);
	
	private MongoAccessor accessor;
	private PiazzaLogger coreLogger;
	private CoreServiceProperties coreServiceProperties;	
	private RestTemplate template;
	
	public DescribeServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProperties, PiazzaLogger coreLogger) {
		this.accessor = accessor;
		this.coreServiceProperties = coreServiceProperties;
		this.template = new RestTemplate();
		this.coreLogger = coreLogger;
	
	}

	/**
	 * Describe service handler
	 */
	public ResponseEntity<String> handle(PiazzaJobType jobRequest) {
		LOGGER.info("Describing a service");
		coreLogger.log("Describing a service", coreLogger.INFO);
		DescribeServiceMetadataJob job = (DescribeServiceMetadataJob) jobRequest;
		ResponseEntity<String> handleResourceReturn = handle(job.serviceID);
		return new ResponseEntity<String>(handleResourceReturn.getBody(), handleResourceReturn.getStatusCode());
	}
	
	public ResponseEntity<String> handle (String serviceId) {
		ResponseEntity<String> responseEntity = null;
		
		try {
	
			Service sMetadata = accessor.getServiceById(serviceId);
			ObjectMapper mapper = new ObjectMapper();
			String result = mapper.writeValueAsString(sMetadata);
			responseEntity = new ResponseEntity<String>(result, HttpStatus.OK);
		} catch (Exception ex) {
			
			LOGGER.error(ex.getMessage());
			coreLogger.log("Could not retrieve resourceId " + serviceId, coreLogger.ERROR);
			responseEntity = new ResponseEntity<String>("Could not retrieve resourceId " + serviceId, HttpStatus.NOT_FOUND);
			
		}
	
		return responseEntity;
		
	}

}
