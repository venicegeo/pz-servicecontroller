package org.venice.piazza.servicecontroller.messaging.handlers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.TextDataType;
import model.data.type.URLParameterDataType;
import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.type.DeleteServiceJob;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;



@RunWith(PowerMockRunner.class)
public class DeleteServiceHandlerTest {
	
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
			
    }

	@Test
	/**
	 * Test that the DeleteServiceHandler constructor is working
	 */
	public void testConstructor() {
		
		DeleteServiceHandler dsh = new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock);
		assertNotNull("The Handler Initialized successfully", dsh);
	}
	
	@Test
	/**
	 * Test that the handle method returns null
	 */
	public void testHandleJobRequestNull() {
		PiazzaJobType jobRequest = null;
		DeleteServiceHandler dsh = new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock);
		ResponseEntity<String> result = dsh.handle(jobRequest);
        assertEquals("The response to a null JobRequest Deletion should be null", result.getStatusCode(), HttpStatus.BAD_REQUEST);
	}
	
	@Test
	/**
	 * Test that handle returns a valid value
	 */
	public void testValidDeletionResponse() {
		
		// Test Response
		String testResponse = "Test Response to see what happens";
		
		// Setup the DeleteServiceJob
		DeleteServiceJob dsj = new DeleteServiceJob();
		dsj.serviceID = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
        dsj.jobId = "fd88cf85-9057-440d-91f0-796d3d398970";
        
        // Try and build a response entity
        ArrayList<String> resultList = new ArrayList<String>();
		resultList.add(dsj.jobId);
		resultList.add(dsj.serviceID);
		ResponseEntity<String> responseEntity = new ResponseEntity<String>(resultList.toString(), HttpStatus.OK); 
		
		// Create a mock and do a return instead of calling the actual handle method
		final DeleteServiceHandler dshMock = Mockito.spy (new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock));
		Mockito.doReturn(testResponse).when(dshMock).handle("a842aae2-bd74-4c4b-9a65-c45e8cd9060", false);
		
		ResponseEntity<String> result = dshMock.handle(dsj);
		assertEquals ("The response entity was correct for the deletion", responseEntity, result);
	}
	
	@Test
	/**
	 * Test what happens when an invalid ID is sent
	 */
	public void testInvalidServiceIdNoDeletion() {
				
		// Setup the DeleteServiceJob
		DeleteServiceJob dsj = new DeleteServiceJob();
		dsj.serviceID = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
        dsj.jobId = "fd88cf85-9057-440d-91f0-796d3d398970";
        
        // Try and build a response entity
        ArrayList<String> resultList = new ArrayList<String>();
		resultList.add(dsj.jobId);
		resultList.add(dsj.serviceID);
		
		// Create a mock and do a return instead of calling the actual handle method
		final DeleteServiceHandler dshMock = Mockito.spy (new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock));
		Mockito.doReturn("").when(dshMock).handle("a842aae2-bd74-4c4b-9a65-c45e8cd9060", false);
		
		ResponseEntity<String> result = dshMock.handle(dsj);
		assertEquals ("The should not be found.", result.getStatusCode(), HttpStatus.NOT_FOUND);
	}
	
	@Test
	/**
	 * Test what happens when an invalid ID is sent
	 */
	public void testInvalidServiceIdNoDeletion2() {
				
		// Setup the DeleteServiceJob
		DeleteServiceJob dsj = new DeleteServiceJob();
		dsj.serviceID = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
        dsj.jobId = "fd88cf85-9057-440d-91f0-796d3d398970";
        
        // Try and build a response entity
        ArrayList<String> resultList = new ArrayList<String>();
		resultList.add(dsj.jobId);
		resultList.add(dsj.serviceID);
		
		// Create a mock and do a return instead of calling the actual handle method
		final DeleteServiceHandler dshMock = Mockito.spy (new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock));
		Mockito.doReturn(null).when(dshMock).handle("a842aae2-bd74-4c4b-9a65-c45e8cd9060", false);
		
		ResponseEntity<String> result = dshMock.handle(dsj);
		assertEquals ("The should not be found.", result.getStatusCode(), HttpStatus.NOT_FOUND);
	}
	
	
	@Test
	/**
	 * Test what happens when an valid service ID is sent
	 */
	public void testSuccessfulDelete() {
				
		String serviceID = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
        
		
		// When calling delete from mongo have it return a successful string
		DeleteServiceHandler dsh = new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock);
		Mockito.doReturn("service " + serviceID + " deleted").when(accessorMock).delete(serviceID, true);

		String result = dsh.handle(serviceID, true);
		// Build the actual result which would be built using ObjectMapper
		String actualResult = "service " + serviceID + " deleted";
		assertEquals ("The serviceID " + serviceID + " should have deleted successfully!", result, actualResult);
	}
	
	@Test
	/**
	 * Test what happens when an valid service ID is sent
	 */
	public void testSuccessfulSoftDelete() {
				
		String serviceID = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
        
		
		// When calling delete from mongo have it return a successful string
		DeleteServiceHandler dsh = new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock);
		Mockito.doReturn("service " + serviceID + " deleted").when(accessorMock).delete(serviceID, false);

		String result = dsh.handle(serviceID, false);
		// Build the actual result which would be built using ObjectMapper
		String actualResult = "service " + serviceID + " deleted";
		assertEquals ("The serviceID " + serviceID + " should have deleted successfully!", result, actualResult);
	}
	
	@Test
	/**
	 * Test what happens when an valid service ID is sent
	 */
	public void testInvalidServiceId() {
				
		String serviceID = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
        
		
		// When calling delete from mongo have it return a successful string
		DeleteServiceHandler dsh = new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock);
		Mockito.doReturn(null).when(accessorMock).delete(serviceID, false);

		String result = dsh.handle(serviceID, false);
	
		assertEquals ("The serviceID " + serviceID + " should have failed deletion!", result, null);
	}
	
	@Test
	/**
	 * Test what happens when an valid service ID is sent
	 */
	public void testInvalidServiceId2() {
				
		String serviceID = "a842aae2-bd74-4c4b-9a65-c45e8cd9060";
        
		
		// When calling delete from mongo have it return a successful string
		DeleteServiceHandler dsh = new DeleteServiceHandler (accessorMock, elasticAccessorMock, coreServicePropMock, piazzaLoggerMock);
		Mockito.doReturn("").when(accessorMock).delete(serviceID, false);

		String result = dsh.handle(serviceID, false);
	
		assertEquals ("The serviceID " + serviceID + " should have failed deletion!", result, "");
	}
}
