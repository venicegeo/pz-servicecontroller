package org.venice.piazza.serviceregistry.controller.messaging.handlers;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.net.URI;
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
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.TextDataType;
import model.data.type.URLParameterDataType;
import model.job.metadata.ResourceMetadata;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import util.PiazzaLogger;

@RunWith(PowerMockRunner.class)
public class ExecuteServiceHandlerTest {
	
	@Mock
	private ExecuteServiceHandler handler;
	
	ResourceMetadata rm = null;
	Service service = null;
	RestTemplate template = null;

	@Before
    public void setup() {
		template = mock(RestTemplate.class);
		try {
			whenNew(RestTemplate.class).withNoArguments().thenReturn(template);
		} catch (Exception e) {
		 	// TODO Auto-generated catch block
				e.printStackTrace();
		}
		rm = new ResourceMetadata();
		rm.name = "toUpper Params";
		rm.description = "Service to convert string to uppercase";

		service = new Service();
		service.method = "POST";
		service.setResourceMetadata(rm);
		service.setServiceId("8");
		service.setUrl("http://localhost:8082/string/toUpper");
    }

	@PrepareForTest({ExecuteServiceHandler.class})
	public void testHandleWithNoInputs() {
		String upperServiceDef = "{  \"name\":\"toUpper Params\"," +
		        "\"description\":\"Service to convert string to uppercase\"," + 
		        "\"url\":\"http://localhost:8082/string/toUpper\"," + 
		         "\"method\":\"POST\"," +
		         "\"params\": [\"aString\"]," + 
		         "\"mimeType\":\"application/json\"" +
		       "}";
		
		ExecuteServiceData edata = new ExecuteServiceData();
		//edata.resourceId = "8";
		edata.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060f");
		
		HashMap<String,DataType> dataInputs = new HashMap<String,DataType>();
		String istring = "The rain in Spain falls mainly in the plain";
		BodyDataType body = new BodyDataType();
		body.content = istring;
		dataInputs.put("Body", body);
		edata.setDataInputs(dataInputs);
		
		URI uri = URI.create("http://localhost:8085//string/toUpper");
		when(template.postForEntity(Mockito.eq(uri),Mockito.any(Object.class),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getServiceById("8")).thenReturn(service);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
	}
	
	//@PrepareForTest({ExecuteServiceHandler.class})
	//@Test
	public void testHandleWithMapInputsPost() {
		ExecuteServiceData edata = new ExecuteServiceData();
		edata.setServiceId("8");
		
		HashMap<String,DataType> dataInputs = new HashMap<String,DataType>();
		TextDataType tdt = new TextDataType();
		tdt.content = "My name is Marge";
		dataInputs.put("name",tdt);
		edata.setDataInputs(dataInputs);
	    URI uri = URI.create("http://localhost:8082/string/toUpper");
		when(template.postForEntity(Mockito.eq(uri),Mockito.any(Object.class),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getServiceById("8")).thenReturn(service);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
	}

	/**
	 * Tests executing web service with GET method
	 */
	@PrepareForTest({ExecuteServiceHandler.class})
	@Test
	public void testHandleWithMapInputsGet() {
		ExecuteServiceData edata = new ExecuteServiceData();
		edata.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060f");
		HashMap<String,DataType> dataInputs = new HashMap<String,DataType>();
		URLParameterDataType tdt = new URLParameterDataType();
		tdt.content = "Marge";

		dataInputs.put("name",tdt);
		edata.setDataInputs(dataInputs);

		ObjectMapper mapper = new ObjectMapper();
		try {
			String tsvc = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(edata);
			System.out.println(tsvc);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    URI uri = URI.create("http://localhost:8082/jumpstart/moviequotewelcome?name=Marge");
		when(template.getForEntity(Mockito.eq(uri),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		// Setup the service
		rm = new ResourceMetadata();
		rm.name = "Movie Quote";
		rm.description = "Service the generates random movie quotes";
        // Setup a service to test GET
		service = new Service();
		service.method = "GET";
		service.setResourceMetadata(rm);
		service.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060");
		service.setUrl("http://localhost:8082/jumpstart//moviequotewelcome");
		when(mockMongo.getServiceById("a842aae2-bd74-4c4b-9a65-c45e8cd9060f")).thenReturn(service);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
	}
}
