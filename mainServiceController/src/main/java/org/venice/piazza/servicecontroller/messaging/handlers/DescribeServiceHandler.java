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
import java.util.ArrayList;
import java.util.List;

/**
 * @author mlynum
 * @version 1.0
 */
import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.type.DescribeServiceMetadataJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DescribeServiceHandler implements PiazzaJobHandler { 
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DescribeServiceHandler.class);
	
	private MongoAccessor accessor;
	private CoreLogger coreLogger;
	private CoreServiceProperties coreServiceProperties;	
	private RestTemplate template;
	
	public DescribeServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProperties, CoreLogger coreLogger) {
		this.accessor = accessor;
		this.coreServiceProperties = coreServiceProperties;
		this.template = new RestTemplate();
		this.coreLogger = coreLogger;
	
	}
	//TODO needs to be implemented
	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest ) {
		
		LOGGER.debug("Describing a service");
		DescribeServiceMetadataJob job = (DescribeServiceMetadataJob)jobRequest;
        ArrayList<String> retVal = new ArrayList<String>();
        ResponseEntity<String> handleResourceReturn = handle(job.serviceID);
        retVal.add(handleResourceReturn.getBody());
        
		return new ResponseEntity<List<String>>(retVal,handleResourceReturn.getStatusCode());
		
	}
	
	public ResponseEntity<String> handle (String resourceId) {
		ResponseEntity<String> responseEntity = null;
		
		try {
	
			ResourceMetadata rMetadata = accessor.getResourceById(resourceId);
			ObjectMapper mapper = new ObjectMapper();
			String result = mapper.writeValueAsString(rMetadata);
			responseEntity = new ResponseEntity<String>(result, HttpStatus.OK);
		} catch (Exception ex) {
			
			LOGGER.error(ex.getMessage());
			responseEntity = new ResponseEntity<String>("Could not retrieve resourceId " + resourceId, HttpStatus.NOT_FOUND);
			
		}
	
		return responseEntity;
		
	}

}
