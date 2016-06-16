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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;

import org.venice.piazza.servicecontroller.util.CoreServiceProperties;


import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.type.DeleteServiceJob;
import model.job.type.UpdateServiceJob;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;


@RunWith(PowerMockRunner.class)
@PrepareForTest({UpdateServiceHandler.class})
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
	
	@Mock
	private UpdateServiceHandler usHandler;

	@Before
    public void setup() {
        // Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toUpper Params";
		rm.description = "Service to convert string to uppercase";

		service = new Service();
		service.method = "POST";
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
	@Ignore
	/**
	 * Test that the handle method returns null
	 */
	public void testHandleJobRequestNull() {
		PiazzaJobType jobRequest = null;
		ResponseEntity<String> result = usHandler.handle(jobRequest);
        assertEquals("The response to a null JobRequest Deletion should be null", result.getStatusCode(), HttpStatus.BAD_REQUEST);
	}
	
	
	/**
	 * Test that handle returns a valid value
	 */
	public void testValidUpdate() {
		UpdateServiceJob job = new UpdateServiceJob();
		job.data = service;
		final UpdateServiceHandler ushMock = Mockito.spy (usHandler);

		Mockito.doReturn("success").when(ushMock).handle(service);
		
		ushMock.handle(job);
	
		//assertEquals ("The response entity was correct for the deletion", responseEntity, result);
	}
	
}
