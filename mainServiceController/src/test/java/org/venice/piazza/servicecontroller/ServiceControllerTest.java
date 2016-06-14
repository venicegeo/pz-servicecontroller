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
package org.venice.piazza.servicecontroller;
/**
 * Class of unit tests to test the deletion of services
 * @author mlynum
 */
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.controller.ServiceController;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.DeleteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.DescribeServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ListServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.SearchServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.UpdateServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.mongodb.DBCursor;

import model.job.metadata.ResourceMetadata;
import model.job.type.RegisterServiceJob;
import model.request.PiazzaJobRequest;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.response.ServiceResponse;
import model.response.SuccessResponse;
import model.service.metadata.Service;
import util.PiazzaLogger;
@RunWith(PowerMockRunner.class)
public class ServiceControllerTest {
	ResourceMetadata rm = null;
	Service service = null;
	
	@InjectMocks
    private ServiceController sc;
	
	@Mock
	private RegisterServiceHandler rsHandlerMock;
	@Mock
	private ExecuteServiceHandler esHandlerMock;
	@Mock
	private DescribeServiceHandler dsHandlerMock;
	@Mock
	private UpdateServiceHandler usHandlerMock;
	@Mock
	private ListServiceHandler lsHandlerMock;
	@Mock
	private DeleteServiceHandler dlHandlerMock;
	@Mock
	private SearchServiceHandler ssHandlerMock;
	@Mock
	private MongoAccessor accessorMock;
	@Mock 
	private ElasticSearchAccessor elasticAccessorMock;
	@Mock
	private CoreServiceProperties coreServicePropMock;
	@Mock 
	private PiazzaLogger loggerMock;
	@Mock
	private DBCursor dbCursorMock;

	
	@Before
	/** 
	 * Called for each test setup
	 */
    public void setup() {
        // Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toUpper Params";
		rm.description = "Service to convert string to uppercase";

		service = new Service();
		service.method = "POST";
		service.setResourceMetadata(rm);
		service.setUrl("http://localhost:8082/string/toUpper");
		MockitoAnnotations.initMocks(this);			

    }
	
	@Test
	/**
	 *  Testing initialization.  This should make sure that nothing crashes
	 */
	public void testInit() {
		ServiceController sc = new ServiceController();
		// Should check to make sure each of the handlers are not null
		sc.initialize();
	}
	@Test
	/**
	 * Test the successful registration of a service
	 */
	public void testRegisterServiceSuccess() {
		
		// Setup the RegisterServiceJob and the PiazzaJobRequest
		PiazzaJobRequest pjr= new PiazzaJobRequest();
		RegisterServiceJob rsj = new RegisterServiceJob();
		rsj.data = service;    
		
		pjr.jobType = rsj;
		pjr.userName = "mlynum";
		service.setServiceId("");
		
		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		Mockito.doReturn(testServiceId).when(rsHandlerMock).handle(rsj.data);

        Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Mockito.anyString());
		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.registerService(pjr);
		assertEquals("The response String should match", ((ServiceResponse)piazzaResponse).serviceId, testServiceId);
	}
	
	@Test
	/**
	 * Test unsuccessful registration
	 */
	public void testRegisterServiceNullJobRequest() {
		
		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.registerService(null);
		assertThat("An ErrorResponse should be returned",piazzaResponse, instanceOf(ErrorResponse.class));
	}
	
	@Test
	/**
	 * Test get service info.
	 */
	public void testGetServiceInfo() {
		
		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		service.setServiceId(testServiceId);
		
        Mockito.doReturn(service).when(accessorMock).getServiceById(testServiceId);
		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.getServiceInfo(testServiceId);
		
		assertThat("SucceessResponse should be returned", piazzaResponse, instanceOf(ServiceResponse.class));
		assertEquals("The response String should match", ((ServiceResponse)piazzaResponse).service.getServiceId(), testServiceId);
	}
	
	@Test
	/**
	 * Test get service info sending a null
	 */
	public void testGetServiceInfoWithNull() {
        Mockito.doThrow(new ResourceAccessException("Service not found.")).when(accessorMock).getServiceById(null);

		PiazzaResponse piazzaResponse = sc.getServiceInfo(null);
		
		assertThat("ErrorResponse should be returned", piazzaResponse, instanceOf(ErrorResponse.class));
	}
	
	@Test
	/** 
	 * Get a list of services
	 */
	public void testGetServices() {
		
		// Get a list of services
		
	}
	
	
    
}
