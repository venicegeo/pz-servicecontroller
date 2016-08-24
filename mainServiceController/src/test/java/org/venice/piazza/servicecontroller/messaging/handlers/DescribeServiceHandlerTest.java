package org.venice.piazza.servicecontroller.messaging.handlers;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.job.Job;
import model.job.metadata.ResourceMetadata;
import model.job.type.DescribeServiceMetadataJob;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;

	public class DescribeServiceHandlerTest {
		
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
		
		@Mock
		private ObjectMapper omMock;
		
		@InjectMocks 
		private DescribeServiceHandler dsHandler;

		@Before
	    public void setup() {
	        // Setup a Service with some Resource Metadata
			rm = new ResourceMetadata();
			rm.name = "toUpper Params";
			rm.description = "Service to convert string to uppercase";

			service = new Service();
			service.setMethod ("POST");
			service.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060");
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
			ResponseEntity<String> result = dsHandler.handle(jobRequest);
	        assertEquals("The response to a null JobRequest update should be null", result.getStatusCode(), HttpStatus.BAD_REQUEST);
		}
		
		/**
		 * Test that the service metadata can be retrieved.
		 */
		@Test
		public void testSuccessDescribe() {
			DescribeServiceMetadataJob dsm = new DescribeServiceMetadataJob();
			String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
			dsm.serviceID = testServiceId;
			try {
				ObjectMapper mapper = new ObjectMapper();
				String responseServiceString = mapper.writeValueAsString(service);

				ResponseEntity<String> responseEntity = new  ResponseEntity<String>(responseServiceString, HttpStatus.OK);
	
				final DescribeServiceHandler dsMock = Mockito.spy (dsHandler);
	
				Job job = new Job();
				job.jobType = dsm;
				
				Mockito.doReturn(responseEntity).when(dsMock).handle(job);				
				ResponseEntity<String> result = dsMock.handle(job);
			
				assertEquals ("The response entity was correct for this describe request", responseEntity, result);
				assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
				assertEquals ("The body of the response is correct", responseEntity.getBody(), responseServiceString);


			} catch (JsonProcessingException jpe) {
				jpe.printStackTrace();
			}

		}
		/**
		 * Test what happens when the service cannot be described
		 */
		@Test
		public void testUnsuccessful() {
			DescribeServiceMetadataJob dsm = new DescribeServiceMetadataJob();
			
			String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
			dsm.serviceID = testServiceId;
			final DescribeServiceHandler dsMock = Mockito.spy (dsHandler);
			ResponseEntity<String> responseEntity = new  ResponseEntity<String>("", HttpStatus.NOT_FOUND);

			Job job = new Job();
			job.jobType = dsm;			
			
			Mockito.doReturn(responseEntity).when(dsMock).handle(testServiceId);			
			ResponseEntity<String> result = dsMock.handle(job);
		
			assertEquals ("The status code should be HttpStatus.NOT_FOUND.", result.getStatusCode(), HttpStatus.NOT_FOUND);

		}
		/**
		 * Test that the service metadata can be retrieved.
		 */
		@Test
		public void testSuccessDescribeService() {
			String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
			try {
				ObjectMapper mapper = new ObjectMapper();
				String responseServiceString = mapper.writeValueAsString(service);	
				ResponseEntity<String> responseEntity = new  ResponseEntity<String>(responseServiceString, HttpStatus.OK);

				Mockito.doReturn(service).when(accessorMock).getServiceById(testServiceId);		
				ResponseEntity<String> result = dsHandler.handle(testServiceId);
			
				assertEquals ("The response entity was correct for this describe request", responseEntity, result);
				assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
				assertEquals ("The body of the response is correct", responseEntity.getBody(), responseServiceString);


			} catch (JsonProcessingException jpe) {
				jpe.printStackTrace();
			}

		}		
		/**
		 * Test that the service metadata could not be retrieved
		 */
		@Test
		public void testUnSuccessDescribeService() {
			String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
			ResponseEntity<String> responseEntity = new  ResponseEntity<String>("null", HttpStatus.OK);

			Mockito.doReturn(null).when(accessorMock).getServiceById(testServiceId);		
			ResponseEntity<String> result = dsHandler.handle(testServiceId);
		
			assertEquals ("The response entity was correct for this describe request", responseEntity, result);
			assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
			assertEquals ("The body of the response is correct", responseEntity.getBody(),  "null");

		}	
		
		/**
		 * Test that the service metadata could not be retrieved 
		 * due to a marshalling error
		 */
		@Test
		public void testUnSuccessDescribeServiceException() {
			String testServiceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
			ResponseEntity<String> responseEntity = new  ResponseEntity<String>("null", HttpStatus.OK);
			try {
				Mockito.when(omMock.writeValueAsString(Mockito.anyString())).thenThrow( new JsonProcessingException("") {});
			} catch (JsonProcessingException jpe) {
				jpe.printStackTrace();
			}
			ResponseEntity<String> result = dsHandler.handle(testServiceId);
		
			assertEquals ("The response entity was correct for this describe request", responseEntity, result);
			assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
			assertEquals ("The body of the response is correct", responseEntity.getBody(),  "null");

		}	
}
