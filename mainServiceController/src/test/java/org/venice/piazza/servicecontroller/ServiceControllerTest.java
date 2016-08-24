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
import static org.hamcrest.CoreMatchers.instanceOf;
/**
 * Class of unit tests to test the deletion of services
 * @author mlynum
 */
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mongojack.JacksonDBCollection;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
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
import org.venice.piazza.servicecontroller.util.TestUtilities;

import com.mongodb.MongoException;

import model.data.DataType;
import model.data.type.BodyDataType;
import model.job.Job;
import model.job.metadata.ResourceMetadata;
import model.job.type.ExecuteServiceJob;
import model.job.type.RegisterServiceJob;
import model.request.PiazzaJobRequest;
import model.response.ErrorResponse;
import model.response.Pagination;
import model.response.PiazzaResponse;
import model.response.ServiceIdResponse;
import model.response.ServiceListResponse;
import model.response.ServiceResponse;
import model.response.SuccessResponse;
import model.service.SearchCriteria;
import model.service.metadata.ExecuteServiceData;
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
	private LocalValidatorFactoryBean validator;
	
	@InjectMocks
	private org.mongojack.DBCursor<Service> dbCursorMock;

	@Mock
	private JacksonDBCollection<Service, String> colMock;

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
		service.setMethod("POST");
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
		//sc.initialize();
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
		pjr.createdBy = "mlynum";
		service.setServiceId("");
		
		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		Mockito.doReturn(testServiceId).when(rsHandlerMock).handle(rsj.data);

        Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Mockito.anyString());
		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.registerService(pjr).getBody();

		assertEquals("The response String should match", ((ServiceIdResponse)piazzaResponse).data.getServiceId(), testServiceId);
	}
	
	@Test
	/**
	 * Test unsuccessful registration
	 */
	public void testRegisterServiceNullJobRequest() {
		
		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.registerService(null).getBody();
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
		PiazzaResponse piazzaResponse = sc.getServiceInfo(testServiceId).getBody();
		
		assertThat("SucceessResponse should be returned", piazzaResponse, instanceOf(ServiceResponse.class));
		assertEquals("The response String should match", ((ServiceResponse)piazzaResponse).data.getServiceId(), testServiceId);
	}
	
	@Test
	/**
	 * Test get service info sending a null
	 */
	public void testGetServiceInfoWithNull() {
        Mockito.doThrow(new ResourceAccessException("Service not found.")).when(accessorMock).getServiceById(null);

		PiazzaResponse piazzaResponse = sc.getServiceInfo(null).getBody();
		
		assertThat("ErrorResponse should be returned", piazzaResponse, instanceOf(ErrorResponse.class));
	}
	
	@Test
	/** 
	 * Get a list of services
	 */
	public void testGetServices() {
		
		// Get a list of services
		List <Service> services = getServicesList();
		// Attach pagination information
		Pagination pagination = new Pagination(1, 1, 1, "serviceId", "asc");

		ServiceListResponse serviceList = new ServiceListResponse(services, pagination);
		// Create some temporary mocks for odd call
		Mockito.when(accessorMock.getServices(1, 25, "asc", "serviceId", "", "")).thenReturn(serviceList);

		PiazzaResponse piazzaResponse = sc.getServices(1, 25, "asc", "serviceId", "", "").getBody();
		assertThat("A list of services should be returned", piazzaResponse, instanceOf(ServiceListResponse.class));
		
	}
	
	@Test
	/** 
	 * Get a list of services with Exception thrown
	 */
	public void testGetServicesThrowException() {
		
		// Get a list of services
		List <Service> services = getServicesList();
		// Attach pagination information
		Pagination pagination = new Pagination(1, 1, 1, "serviceId", "asc");

		// Create some temporary mocks for odd call
		Mockito.when(accessorMock.getServices(1, 25, "asc", "serviceId", "", "")).thenThrow(new MongoException("There was an error"));

		PiazzaResponse piazzaResponse = sc.getServices(1, 25, "asc", "serviceId", "", "").getBody();
		assertThat("A list of services should be returned", piazzaResponse, instanceOf(ErrorResponse.class));
		
	}
	
	@Test
	/**
	 * Test the successful un-registration of a service
	 */
	public void testUnregisterServiceSuccess() {
		
		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		Mockito.doReturn(testServiceId).when(dlHandlerMock).handle(testServiceId, false);

		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.unregisterService(testServiceId, false).getBody();
		assertThat("The unregistration  should be successful",piazzaResponse, instanceOf(SuccessResponse.class));

	}
	
	@Test
	/**
	 * Test the successful un-registration of a service
	 */
	public void testUnregisterServiceSuccessSD() {
		
		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		Mockito.doReturn(testServiceId).when(dlHandlerMock).handle(testServiceId, true);

		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.unregisterService(testServiceId, true).getBody();
		assertThat("The unregistration  should be successful",piazzaResponse, instanceOf(SuccessResponse.class));

	}
	
	@Test
	/**
	 * Test unsuccessful un-registration
	 */
	public void testUnregisterServiceServiceId() {
		
		// Should check to make sure each of the handlers are not null
		Mockito.doThrow(new MongoException("Error")).when(dlHandlerMock).handle(null, false);

		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.unregisterService(null, false).getBody();
		

		assertThat("An ErrorResponse should be returned", piazzaResponse, instanceOf(ErrorResponse.class));
	}
	
	@Test
	/**
	 * Test unsuccessful un-registration soft delete
	 */
	public void testUnregisterServiceServiceIdSD() {
		
		// Should check to make sure each of the handlers are not null
		Mockito.doThrow(new MongoException("Error")).when(dlHandlerMock).handle(null, true);

		// Should check to make sure each of the handlers are not null
		PiazzaResponse piazzaResponse = sc.unregisterService(null, true).getBody();
		

		assertThat("An ErrorResponse should be returned", piazzaResponse, instanceOf(ErrorResponse.class));
	}

	@Test
	public void testUpdateServiceMetadata() throws Exception{

		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		service.setServiceId(testServiceId);
		Mockito.doReturn("Update Successful").when(usHandlerMock).handle(service);
		Mockito.doReturn(service).when(accessorMock).getServiceById(Mockito.eq("9a6baae2-bd74-4c4b-9a65-c45e8cd9060"));

		ResponseEntity<PiazzaResponse> piazzaResponse = sc.updateServiceMetadata(testServiceId, service);
		assertThat("The update of service metadata should be successful", piazzaResponse.getBody(), instanceOf(SuccessResponse.class));
	}

	@Test
	/**
	 * Test that the service is not updated when the serviceIds do not match
	 */
	public void testUpdateServiceMetadataNoMatch() {
		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		service.setServiceId("123-23323bsr");
		Mockito.doReturn("Update Successful").when(usHandlerMock).handle(service);
		Mockito.doReturn(service).when(accessorMock).getServiceById(Mockito.eq(testServiceId));

		ResponseEntity<PiazzaResponse> piazzaResponse = sc.updateServiceMetadata(testServiceId, service);
		assertThat("The update of service metadata should be  successful", piazzaResponse.getBody(), instanceOf(SuccessResponse.class));
	}

	@Test
	/**
	 * Test that a PiazzaError Response is returned when the update does not
	 * work
	 */
	public void testUpdateServiceMetadataNoResult() {

		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		service.setServiceId(testServiceId);
		Mockito.doReturn("").when(usHandlerMock).handle(service);
		Mockito.doReturn(service).when(accessorMock).getServiceById(Mockito.eq(testServiceId));

		ResponseEntity<PiazzaResponse> piazzaResponse = sc.updateServiceMetadata(testServiceId, service);
		assertThat("The update of service metadata should be unsuccessful", piazzaResponse.getBody(), instanceOf(ErrorResponse.class));
	}

	@Test
	/**
	 * Test that a PiazzaError Response is returned when an Exception is thrown
	 */
	public void testUpdateServiceMetadataExceptionThrown() {

		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		service.setServiceId(testServiceId);
		Mockito.doThrow(new MongoException("There was an error")).when(usHandlerMock).handle(service);
		Mockito.doReturn(service).when(accessorMock).getServiceById(Mockito.eq(testServiceId));

		ResponseEntity<PiazzaResponse> piazzaResponse = sc.updateServiceMetadata(testServiceId, service);
		assertThat("The update of service metadata should be unsuccessful", piazzaResponse.getBody(), instanceOf(ErrorResponse.class));
	}
	
	@Test
	/**
	 * Update Service Info
	 */
	public void testUpdateService() {
		
		String testServiceId = "9a6baae2-bd74-4c4b-9a65-c45e8cd9060";
		service.setServiceId(testServiceId);
		Mockito.doReturn(testServiceId).when(usHandlerMock).handle(service);

		String result = sc.updateService(service);
		assertTrue("The serviceId should be in the response", result.contains(testServiceId));


	}
	@Test
	/**
	 * Test Executing a service
	 */
	public void testExecuteService() {
		ExecuteServiceData edata = new ExecuteServiceData();
		//edata.resourceId = "8";
		edata.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060f");
		
		HashMap<String,DataType> dataInputs = new HashMap<String,DataType>();
		String istring = "The rain in Spain falls mainly in the plain";
		BodyDataType body = new BodyDataType();
		body.content = istring;
		dataInputs.put("Body", body);
		edata.setDataInputs(dataInputs);
		
		String responseString = "\"jobId:1234567\"";
		ResponseEntity<String> responseEntity = new ResponseEntity<String>(responseString, HttpStatus.OK); 
        Mockito.doReturn(responseEntity).when(esHandlerMock).handle(edata, "sakuser");
		
		ResponseEntity<String> retVal = sc.executeService(edata);
        assertEquals("The response should be the same", responseString, retVal.getBody());
	}
	
	@Test
	/**
	 * Test Executing a service throwing an Exception
	 */
	public void testExecuteServiceThrowException() {
		ExecuteServiceData edata = new ExecuteServiceData();
		//edata.resourceId = "8";
		edata.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060f");
		
		HashMap<String,DataType> dataInputs = new HashMap<String,DataType>();
		String istring = "The rain in Spain falls mainly in the plain";
		BodyDataType body = new BodyDataType();
		body.content = istring;
		dataInputs.put("Body", body);
		edata.setDataInputs(dataInputs);
		
		ExecuteServiceJob esj = new ExecuteServiceJob();
		esj.data = edata;
		
		Job job = new Job();
		job.jobType = esj;
		
        Mockito.doThrow(new MongoException("An error occured")).when(esHandlerMock).handle(job);
		
		ResponseEntity<String> retVal = sc.executeService(edata);
        assertEquals("The response should be a null", retVal, null);
	}
	
	@Test
	/**
	 * tests the describeService.  This is called internally for testing
	 */
	
	public void testDescribeService() {
		String serviceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060f";
		ResponseEntity<String> responseEntity = new ResponseEntity<String>("Just a test to describe" + serviceId, HttpStatus.OK); 
        Mockito.doReturn(responseEntity).when(dsHandlerMock).handle(serviceId);
        ResponseEntity<String> result = sc.describeService(serviceId);
        assertEquals("The response should be 200", result.getStatusCode(), responseEntity.getStatusCode());
        assertTrue(result.getBody().contains(serviceId));

	}
	
	@Test
	/**
	 * tests the deleteService.  This is called internally for testing
	 */
	
	public void testDeleteService() {
		String serviceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060f";
		ResponseEntity<String> responseEntity = new ResponseEntity<String>("Just a test to delete " + serviceId, HttpStatus.OK); 
        Mockito.doReturn("Just a test to describe" + serviceId).when(dlHandlerMock).handle(serviceId, false);
        ResponseEntity<String> result = sc.deleteService(serviceId);
        assertEquals("The response should be 200", result.getStatusCode(), responseEntity.getStatusCode());
        assertTrue(result.getBody().contains(serviceId));
	}

	@Test
	/**
	 * tests the listService.  This is called internally for testing
	 */
	
	public void testListService() {
		ResponseEntity<String> responseEntity = new ResponseEntity<String>("Just a test to list", HttpStatus.OK); 
        Mockito.doReturn(responseEntity ).when(lsHandlerMock).handle();
        ResponseEntity<String> result = sc.listService();
        assertEquals("The response should be 200", result.getStatusCode(), responseEntity.getStatusCode());
	}
	
	@Test
	/**
	 * tests the search.  This is called internally for testing
	 */
	
	public void testSearch() {
		SearchCriteria criteria = new SearchCriteria();
		criteria.field = "name";
		criteria.pattern = "M*";
		
		ResponseEntity<String> responseEntity = new ResponseEntity<String>("Just a test to search", HttpStatus.OK); 
        Mockito.doReturn(responseEntity ).when(ssHandlerMock).handle(criteria);
        ResponseEntity<String> result = sc.search(criteria);
        assertEquals("The response should be 200", result.getStatusCode(), responseEntity.getStatusCode());
	}
	@Test
	/**
	 * test health check
	 */
	public void testHealthCheck() {
		String htmlMessage = "<HTML><TITLE>Piazza Service Controller Welcome</TITLE>";
		htmlMessage = htmlMessage + "<BODY><BR> Welcome from the Piazza Service Controller. "
				+ "<BR>For details on running and using the ServiceController, "
				+ "<BR>see <A HREF=\"http://pz-docs.geointservices.io/devguide/index.html\"> The Piazza Developer's Guide<A> for details."
				+ "<BODY></HTML>";
		ResponseEntity<String> result = sc.healthCheck();
        assertEquals("The response should be 200", result.getStatusCode(), HttpStatus.OK);
        assertTrue("The response contains the appropriate message", result.getBody().contains(htmlMessage));
	}
	/**
	 * Return a list of generic services for testing.  
	 * Each service has a unique serviceId
	 * @return
	 */
	private List <Service> getServicesList() {
		List <Service> services = new ArrayList <> ();
		
		for (int i =0; i < 10; i++) {
		
			// Setup a Service with some Resource Metadata
			ResourceMetadata rm = new ResourceMetadata();
			rm.name = "toUpper Params";
			rm.description = "Service to convert string to uppercase";
	
			service = new Service();
			
			service.setServiceId("9a6baae" + TestUtilities.randInt(0,  9) + 
					"-bd" + TestUtilities.randInt(0, 9) +
					"4-4c4b-9a65-c45e" + TestUtilities.randInt(0, 9) + "cd" + 
					TestUtilities.randInt(0,  9) + "060");
			service.setMethod("POST");
			service.setResourceMetadata(rm);
			service.setUrl("http://localhost:8082/string/toUpper");
			services.add(service);
		}
		return services;
	}
	    
}
