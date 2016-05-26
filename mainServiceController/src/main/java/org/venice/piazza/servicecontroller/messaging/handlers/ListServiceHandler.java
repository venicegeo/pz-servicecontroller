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
import model.service.metadata.Service;
import util.PiazzaLogger;

public class ListServiceHandler implements PiazzaJobHandler { 
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ListServiceHandler.class);
	
	private MongoAccessor accessor;
	private PiazzaLogger coreLogger;
	private CoreServiceProperties coreServiceProperties;	
	private RestTemplate template;
	
	public ListServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProperties, PiazzaLogger coreLogger) {
		this.accessor = accessor;
		this.coreServiceProperties = coreServiceProperties;
		this.template = new RestTemplate();
		this.coreLogger = coreLogger;
	
	}
	
	/**
	 * ListService handler
	 */
	public ResponseEntity<String> handle (PiazzaJobType jobRequest ) {
		LOGGER.info("Listing services");
		coreLogger.log("listing service", coreLogger.INFO);
        ResponseEntity<String> handleResourceReturn = handle();
        
		return new ResponseEntity<String>(handleResourceReturn.getBody(),handleResourceReturn.getStatusCode());
	}
	
	public ResponseEntity<String> handle () {
		ResponseEntity<String> responseEntity = null;
		
		try {
	
			List<Service> rmList = accessor.list();
			ObjectMapper mapper = new ObjectMapper();
			String result = mapper.writeValueAsString(rmList);
			responseEntity = new ResponseEntity<String>(result, HttpStatus.OK);
		} catch (Exception ex) {
			
			LOGGER.error(ex.getMessage());
			coreLogger.log(ex.getMessage(), coreLogger.ERROR);
			responseEntity = new ResponseEntity<String>("Could not retrieve list of service metadata " , HttpStatus.NOT_FOUND);
			
		}
	
		return responseEntity;
		
	}

}
