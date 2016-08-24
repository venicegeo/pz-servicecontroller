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
 * Class for testing the UpdateServiceHandler
 * @author mlynum
 *
 */
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;

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
import model.job.type.UpdateServiceJob;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;


public class UpdateServiceHandlerTest {
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
	private UpdateServiceHandler usHandler;

	@Before
    public void setup() {
        // Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toUpper Params";
		rm.description = "Service to convert string to uppercase";

		service = new Service();
		service.setMethod("POST");
		service.setResourceMetadata(rm);
		service.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060");
		service.setUrl("http://localhost:8082/string/toUpper");
		MockitoAnnotations.initMocks(this);			
    }
	
	@Test
	/**
	 * Test that the DeleteServiceHandler constructor is working
	 */
	public void testConstructor() {
		assertNotNull("The Handler Initialized successfully", usHandler);
	}
	
	@Test
	/**
	 * Test that the handle method returns null
	 */
	public void testHandleJobRequestNull() {
		Job jobRequest = new Job();
		ResponseEntity<String> result = usHandler.handle(jobRequest);
        assertEquals("The response to a null JobRequest update should be null", result.getStatusCode(), HttpStatus.BAD_REQUEST);
	}
	
	
	/**
	 * Test that handle returns a valid value
	 */
	@Test
	public void testValidUpdate() {
		UpdateServiceJob usj = new UpdateServiceJob();
		usj.data = service;
		usj.jobId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
		
		ArrayList<String> resultList = new ArrayList<String>();
		resultList.add(usj.jobId);
		resultList.add(service.getServiceId());
		
		ResponseEntity<String> responseEntity = new  ResponseEntity<String>(resultList.toString(), HttpStatus.OK);

		final UpdateServiceHandler ushMock = Mockito.spy (usHandler);

		Mockito.doReturn("success").when(ushMock).handle(service);
		
		Job job = new Job();
		job.jobType = usj;
		
		ResponseEntity<String> result = ushMock.handle(job);
	
		assertEquals ("The response entity was correct for the update", responseEntity, result);

	}
	
	/**
	 * Test what happens when there is an invalid update
	 */
	@Test
	public void testUnsuccessfulUpdate() {
		UpdateServiceJob usj = new UpdateServiceJob();
		usj.data = service;
		usj.jobId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
		
		ArrayList<String> resultList = new ArrayList<String>();
		resultList.add(usj.jobId);
		resultList.add(service.getServiceId());
		
		ResponseEntity<String> responseEntity = new  ResponseEntity<String>(resultList.toString(), HttpStatus.UNPROCESSABLE_ENTITY);

		final UpdateServiceHandler ushMock = Mockito.spy (usHandler);

		Mockito.doReturn("").when(ushMock).handle(service);
		
		Job job = new Job();
		job.jobType = usj;		
		
		ResponseEntity<String> result = ushMock.handle(job);
	
		assertEquals ("The item was not updated successfully.", responseEntity.getStatusCode(), result.getStatusCode());

	}
	
	/**
	 * Test whether the service is updated successfully by sending in
	 * direct service information
	 */
	@Test
	public void testHandleNullService() {
		Service testService = null;
		String result = usHandler.handle(testService);
        assertEquals("The response string should be empty", result.length(), 0);
	}
	
	
	/**
	 * Test whether information is updated successfully
	 */
	@Test
	public void testHandleService() {
		// Mock the response from Mongo
		Mockito.doReturn(service.getServiceId()).when(accessorMock).update(service);
		String result = usHandler.handle(service);
        assertEquals("The responding service id shoudl match the id", result, service.getServiceId());
	}
	
	/**
	 * Test unsuccessful update of information
	 */
	@Test
	public void testUnsucessfulUpdateServiceInfo() {
		// Mock the response from Mongo
		Mockito.doReturn("").when(accessorMock).update(service);
		String result = usHandler.handle(service);
        assertEquals("The response string should be empty", result.length(), 0);
	}
	
	
	/**
	 * Test contract information
	 */
	@Test
	public void testContractInfo() {
		service.setContractUrl("http://pzsvc-hello.int.geointservices.io/");
		// Mock the response from Mongo
		Mockito.doReturn(service.getServiceId()).when(accessorMock).update(service);
		String result = usHandler.handle(service);
        assertEquals("The responding service id shoudl match the id", result, service.getServiceId());
	}

}
