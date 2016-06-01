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


import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.job.PiazzaJobType;
import model.job.type.RegisterServiceJob;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;


/**
 * Handler for handling registerService requests.  This handler is used 
 * when register-service kafka topics are received or when clients utilize the 
 * ServiceController registerService web service.
 * @author mlynum
 * @version 1.0
 *
 */

public class RegisterServiceHandler implements PiazzaJobHandler {
	private MongoAccessor mongoAccessor;
	private ElasticSearchAccessor elasticAccessor;
	private PiazzaLogger coreLogger;
	private UUIDFactory uuidFactory;
	private static final Logger LOGGER = LoggerFactory.getLogger(RegisterServiceHandler.class);
	private RestTemplate template;


	public RegisterServiceHandler(MongoAccessor mongoAccessor, ElasticSearchAccessor elasticAccessor,CoreServiceProperties coreServiceProp, PiazzaLogger coreLogger, UUIDFactory uuidFactory){ 
		this.mongoAccessor = mongoAccessor;
		this.elasticAccessor = elasticAccessor;
		this.coreLogger = coreLogger;
		this.uuidFactory = uuidFactory;
		this.template = new RestTemplate();
	
	}

    /**
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in MongoDB
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	@SuppressWarnings("deprecation")
	public ResponseEntity<String> handle(PiazzaJobType jobRequest) {
		coreLogger.log("Registering a Service", PiazzaLogger.INFO);
		RegisterServiceJob job = (RegisterServiceJob) jobRequest;

		if (job != null) {
			// Get the Service metadata
			Service serviceMetadata = job.data;
			LOGGER.info("serviceMetadata received is " + serviceMetadata);
			coreLogger.log("serviceMetadata received is " + serviceMetadata, PiazzaLogger.INFO);

			String result = handle(serviceMetadata);
			if (result.length() > 0) {
				String responseString = "{\"resourceId\":" + "\"" + result + "\"}";
				return new ResponseEntity<String>(responseString, HttpStatus.OK);
			} else {
				LOGGER.error("No result response from the handler, something went wrong");
				coreLogger.log("No result response from the handler, something went wrong", PiazzaLogger.ERROR);
				return new ResponseEntity<String>("RegisterServiceHandler handle didn't work", HttpStatus.METHOD_FAILURE);
			}

		} else {
			LOGGER.error("No RegisterServiceJob");
			coreLogger.log("No RegisterServiceJob", PiazzaLogger.ERROR);
			return new ResponseEntity<String>("No RegisterServiceJob", HttpStatus.METHOD_FAILURE);
		}
	}

	/**
	 * 
	 * @param rMetadata
	 * @return resourceID of the registered service
	 */
	public String handle (Service sMetadata) {

        //coreLogger.log("about to save a registered service.", PiazzaLogger.INFO);
		/*TODO if ever decide to add more detailed metadata
		 if (sMetadata.getContractUrl() != null) {
			UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getContractUrl());
			URI url = URI.create(builder.toUriString());
			ResponseEntity<String> responseEntity  = template.getForEntity(url, String.class);
			if (responseEntity.getStatusCode() == HttpStatus.OK && responseEntity.hasBody()) {
				sMetadata.setContractData(responseEntity.getBody());
			}
			else {
				LOGGER.warn("Unable to get contract data");
			}
		}*/
		sMetadata.setServiceId(uuidFactory.getUUID());
		String result = mongoAccessor.save(sMetadata);
		LOGGER.debug("The result of the save is " + result);
		PiazzaResponse response = elasticAccessor.save(sMetadata);
		if (ErrorResponse.class.isInstance(response)) {
			ErrorResponse errResponse = (ErrorResponse)response;
			LOGGER.error("The result of the save is " + errResponse.message);
		}
		else {
			LOGGER.debug("Successfully stored service " + sMetadata.getServiceId());
		}
		return sMetadata.getServiceId();
	}
	


}
