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
/**
 * Class for testing the RegisterServiceHandler
 * @author mlynum
 *
 */
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;

import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.job.Job;
import model.job.metadata.ResourceMetadata;
import model.job.type.RegisterServiceJob;
import model.response.ErrorResponse;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;
public class RegisterServiceHandlerTest {
	
	ResourceMetadata rm = null;
	Service service = null;
	
	// Create some mocks
	@Mock
	private MongoAccessor accessorMock;
	
	@Mock 
	private ElasticSearchAccessor elasticAccessorMock;
	
	@Mock
	private CoreServiceProperties coreServicePropMock;
	
	@Mock 
	private PiazzaLogger piazzaLoggerMock;
	
	@Mock
	private UUIDFactory uuidFactoryMock;
	
	@InjectMocks 
	private RegisterServiceHandler rsHandler;

	@Before
    public void setup() {
        // Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toUpper Params";
		rm.description = "Service to convert string to uppercase";

		service = new Service();
		service.setMethod ("POST");
		service.setResourceMetadata(rm);
		service.setUrl("http://localhost:8082/string/toUpper");
		MockitoAnnotations.initMocks(this);			
    }
	
	@Test
	/**
	 * Test what happens when a null is sent to register a service
	 */
	public void testHandleJobRequestNull() {
		Job jobRequest = new Job();
		ResponseEntity<String> result = rsHandler.handle(jobRequest);
        assertEquals("The response to a null JobRequest update should be null", result.getStatusCode(), HttpStatus.BAD_REQUEST);
	}

	/**
	 * Test that the service is successfully registered
	 */
	@Test
	public void testSuccessRegistration() {
		RegisterServiceJob rsj = new RegisterServiceJob();
		String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
		rsj.data = service;

		String responseString = "{\"resourceId\":" + "\"" + testServiceId + "\"}";


		ResponseEntity<String> responseEntity = new  ResponseEntity<String>(responseString, HttpStatus.OK);

		final RegisterServiceHandler rshMock = Mockito.spy (rsHandler);

		Mockito.doReturn(testServiceId).when(rshMock).handle(service);
		
		Job job = new Job();
		job.jobType = rsj;
		
		ResponseEntity<String> result = rshMock.handle(job);
	
		assertEquals ("The response entity was correct for this registration", responseEntity, result);
		assertEquals ("The status code should be 200", responseEntity.getStatusCode(), HttpStatus.OK);
		assertEquals ("The body of the response entity is correct", responseEntity.getBody(), responseString);


	}
	
	/**
	 * Test what happens when there is an invalid registration
	 */
	@Test
	public void testUnsuccessfulRegistration() {
		RegisterServiceJob rsj = new RegisterServiceJob();
		
		rsj.data = service;

		
		final RegisterServiceHandler rsMock = Mockito.spy (rsHandler);

		Mockito.doReturn("").when(rsMock).handle(service);
		
		Job job = new Job();
		job.jobType = rsj;		
		
		ResponseEntity<String> result = rsMock.handle(job);
	
		assertEquals ("The status code should be HttpStatus.UNPROCESSABLE_ENTITY.", result.getStatusCode(), HttpStatus.UNPROCESSABLE_ENTITY);

	}
	
	/**
	 * Test whether the service is registered successfully by sending in
	 * direct service information
	 */
	@Test
	public void testHandleNullService() {
		Service testService = null;
		String result = rsHandler.handle(testService);
        assertEquals("The serviceId string returned should be empty", result.length(), 0);
	}
	
	/**
	 * Test whether information is registered successfully
	 */
	@Test
	public void testHandleService() {
		String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
		// Mock the response from Mongo
		Mockito.doReturn(testServiceId).when(accessorMock).save(service);
		String result = rsHandler.handle(service);
        assertEquals("The responding service id should match the id", result, testServiceId);
	}
	
	/**
	 * Test what happens when there is an error with registering
	 */
	@Test
	public void testBadRegistration() {
		String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";

		// Mock the response from Mongo
		Mockito.when(uuidFactoryMock.getUUID()).thenReturn(testServiceId);
		Mockito.doReturn("").when(accessorMock).save(service);
		String result = rsHandler.handle(service);
        assertEquals("The serviceId string returned should be empty", result.length(), 0);

	}
	
	/**
	 * Test what happens when there is an error with registering
	 * with elasticsearch
	 */
	@Test
	public void testElasticSearchSaveProblem() {
		String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";

		// Mock the response from Mongo
		Mockito.when(uuidFactoryMock.getUUID()).thenReturn(testServiceId);
		Mockito.doReturn(testServiceId).when(accessorMock).save(service);
		ErrorResponse errorResponse = new ErrorResponse();
		errorResponse.message = "There was a problem";
		Mockito.doReturn(errorResponse).when(elasticAccessorMock).save(service);
		String result = rsHandler.handle(service);
		assertEquals("The responding service id should match the id", result, testServiceId);

	}
}
