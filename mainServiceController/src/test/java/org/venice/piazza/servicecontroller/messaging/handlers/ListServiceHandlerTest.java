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
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.job.Job;
import model.job.metadata.ResourceMetadata;
import model.job.type.ListServicesJob;
import model.service.metadata.Service;
import util.PiazzaLogger;

/**
 * @author mlynum
 * @version 1.0
 */


public class ListServiceHandlerTest {
	
	List <Service> services = null;

	
	@InjectMocks 
	private PiazzaLogger coreLoggerMock;
	@Mock
	private MongoAccessor accessorMock;
	@Mock 
	private ElasticSearchAccessor elasticAccessorMock;
	@Mock
	private CoreServiceProperties coreServicePropMock;
	@Mock 
	private PiazzaLogger piazzaLoggerMock;
	@Mock
	private ObjectMapper omMock;
	
	@InjectMocks
	private ListServiceHandler lsHandler;
	
	@Before
    public void setup() {
		
		// Create some services
		Service service;
		ResourceMetadata rm;
		services = new ArrayList<>();
        // Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toUpper Params";
		rm.description = "Service to convert string to uppercase";

		service = new Service();
		service.setMethod ("POST");
		service.setResourceMetadata(rm);
		service.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060");
		service.setUrl("http://localhost:8082/jumpstart/string/toUpper");
		MockitoAnnotations.initMocks(this);	
		
		services.add(service);
		
		// Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toLower Params";
		rm.description = "Service to convert string to lower";

		service = new Service();
		service.setMethod ("POST");
		service.setResourceMetadata(rm);
		service.setServiceId("a842bte2-bd74-4c4b-9a65-c45e8cdu91");
		service.setUrl("http://localhost:8082/jumpstart/string/toLower");
		services.add(service);
		
		rm = new ResourceMetadata();
		rm.name = "convert";
		rm.description = "Converts a string to UPPER or LOWER case. POST method";
		
		service = new Service();
		service.setMethod ("POST");
		service.setResourceMetadata(rm);
		service.setServiceId("9842bze2-bd74-4c4b-0a65-c45e8cdu91");
		service.setUrl("http://localhost:8082/umpstart/string/convert");
		
		services.add(service);
		MockitoAnnotations.initMocks(this);	
		
    }
	/**
	 * Test that a list of services could be retrieved.
	 */
	@Test
	public void testListServicesSuccess() {
		ListServicesJob lsj = new ListServicesJob();
	
		try {
			ObjectMapper mapper = new ObjectMapper();
			String responseServiceString = mapper.writeValueAsString(services);

			ResponseEntity<String> responseEntity = new  ResponseEntity<String>(responseServiceString, HttpStatus.OK);

			final ListServiceHandler lsMock = Mockito.spy (lsHandler);

			Mockito.doReturn(responseEntity).when(lsMock).handle();		
			Job job = new Job();
			job.jobType = lsj;			
			
			ResponseEntity<String> result = lsMock.handle(job);
		
			assertEquals ("The response entity was correct for this describe request", responseEntity, result);
			assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
			assertEquals ("The body of the response is correct", responseEntity.getBody(), responseServiceString);

		} catch (JsonProcessingException jpe) {
			jpe.printStackTrace();
		}

	}
	
	/**
	 * Test that a list of services could not be retrieved.
	 */
	@Test
	public void testListServicesFailure() {
		ListServicesJob lsj = new ListServicesJob();

		ResponseEntity<String> responseEntity = new  ResponseEntity<String>("", HttpStatus.NOT_FOUND);

		final ListServiceHandler lsMock = Mockito.spy (lsHandler);

		Mockito.doReturn(responseEntity).when(lsMock).handle();
		
		Job job = new Job();
		job.jobType = lsj;		
		
		ResponseEntity<String> result = lsMock.handle(job);
	
		assertEquals ("The status code should be HttpStatus.NOT_FOUND.", result.getStatusCode(), HttpStatus.NOT_FOUND);

	}
	
	/**
	 * Test that a list of services could be retrieved.
	 */
	@Test
	public void testListServicesSuccessAccessor() {	
		try {
			ObjectMapper mapper = new ObjectMapper();
			String responseServiceString = mapper.writeValueAsString(services);

			ResponseEntity<String> responseEntity = new  ResponseEntity<String>(responseServiceString, HttpStatus.OK);
			Mockito.doReturn(services).when(accessorMock).list();		

			ResponseEntity<String> result = lsHandler.handle();
		
			assertEquals ("The response entity was correct for this list services request", responseEntity, result);
			assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
			assertEquals ("The body of the response is correct", responseEntity.getBody(), responseServiceString);


		} catch (JsonProcessingException jpe) {
			jpe.printStackTrace();
		}

	}
	
	/**
	 * Test what happens when there are no registered user services
	 */
	@Test
	public void testEmptyList() {	
		try {
			ObjectMapper mapper = new ObjectMapper();
			
			List <Service> serviceList = new ArrayList<>();
			String responseServiceString = mapper.writeValueAsString(serviceList);

			ResponseEntity<String> responseEntity = new  ResponseEntity<String>(responseServiceString, HttpStatus.OK);
			Mockito.doReturn(serviceList).when(accessorMock).list();		

			ResponseEntity<String> result = lsHandler.handle();
		
			assertEquals ("The response entity was correct for this list services request", responseEntity, result);
			assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
			assertEquals ("The body of the response is correct with empty list", responseEntity.getBody(), responseServiceString);

		} catch (JsonProcessingException jpe) {
			jpe.printStackTrace();
		}

	}
	
	/**
	 * Test that the list of services could not be retrieved 
	 * due to a marshalling error
	 */
	@Test
	public void testUnsuccessListServiceException() {
		ResponseEntity<String> responseEntity = new  ResponseEntity<String>("Could not retrieve a list of user services", HttpStatus.NOT_FOUND);
		try {
			final ListServiceHandler lsMock = Mockito.spy (lsHandler);

			Mockito.doReturn(omMock).when(lsMock).makeObjectMapper();
			Mockito.doReturn(services).when(accessorMock).list();	
			
			Mockito.when(omMock.writeValueAsString(services)).thenThrow( new JsonMappingException("Test Exception") );
			ResponseEntity<String> result = lsMock.handle();

			
			assertEquals ("The response entity was correct for this list service request", responseEntity, result);
			assertEquals ("The response code is 404", responseEntity.getStatusCode(), HttpStatus.NOT_FOUND);
			assertEquals ("The body of the response is correct", responseEntity.getBody(),  "Could not retrieve a list of user services");
		} catch (JsonProcessingException jpe) {
			jpe.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	

}
