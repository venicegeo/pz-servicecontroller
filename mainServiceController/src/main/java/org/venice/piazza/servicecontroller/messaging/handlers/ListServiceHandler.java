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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.databind.ObjectMapper;

import model.job.Job;
import model.service.metadata.Service;
import util.PiazzaLogger;

/**
 * @author mlynum
 * @version 1.0
 */
@Component
public class ListServiceHandler implements PiazzaJobHandler { 
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ListServiceHandler.class);
	
	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private PiazzaLogger coreLogger;
	
	/**
	 * ListService handler
	 */
	@Override
	public ResponseEntity<String> handle (Job jobRequest ) {
		coreLogger.log("listing service", PiazzaLogger.INFO);
        ResponseEntity<String> handleResourceReturn = handle();
        if (handleResourceReturn.getBody().length() > 0) {
        	return new ResponseEntity<>(handleResourceReturn.getBody(), handleResourceReturn.getStatusCode());
		} else {
			coreLogger.log("Something went wrong when trying to get a list of services", PiazzaLogger.ERROR);
			return new ResponseEntity<>("Could not retrieve a list of user services", HttpStatus.NOT_FOUND);
		}
	}
	
	public ResponseEntity<String> handle () {
		ResponseEntity<String> responseEntity = null;
		try {
			List<Service> rmList = accessor.list();
			ObjectMapper mapper = makeObjectMapper();
			String result = mapper.writeValueAsString(rmList);
			responseEntity = new ResponseEntity<String>(result, HttpStatus.OK);
		} catch (Exception ex) {
			coreLogger.log(ex.getMessage(), PiazzaLogger.ERROR);
			responseEntity = new ResponseEntity<String>("Could not retrieve a list of user services" , HttpStatus.NOT_FOUND);
		}

		return responseEntity;
	}
	
	ObjectMapper makeObjectMapper() {
		return new ObjectMapper();
	}
}