package org.venice.piazza.serviceregistry.controller.messaging.handlers;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.job.metadata.ExecuteServiceData;
import model.job.metadata.ResourceMetadata;
import util.PiazzaLogger;



@RunWith(PowerMockRunner.class)
public class TestExecuteServiceHandler {
	ResourceMetadata rm = null;
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
		rm.url = "http://localhost:8082/string/toUpper";
		rm.method = "POST";
		rm.requestMimeType = "application/json";
		rm.id="Cheese";
    	
    }
	@PrepareForTest({ExecuteServiceHandler.class})
	@Test
	public void testHandleWithNoInputs() {
		/*String upperServiceDef = "{  \"name\":\"toUpper Params\"," +
		        "\"description\":\"Service to convert string to uppercase\"," + 
		        "\"url\":\"http://localhost:8082/string/toUpper\"," + 
		         "\"method\":\"POST\"," +
		         "\"params\": [\"aString\"]," + 
		         "\"mimeType\":\"application/json\"" +
		       "}";*/
		
		ExecuteServiceData edata = new ExecuteServiceData();
		edata.resourceId = "8";
		edata.setDataInputs(new HashMap<String,String>());
		edata.setDataInput("");
	
		when(template.postForEntity("http://localhost:8082/string/toUpper",null,String.class)).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getResourceById("8")).thenReturn(rm);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ExecuteServiceHandler handler = new ExecuteServiceHandler(mockMongo,props,logger);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
	}
	
	@PrepareForTest({ExecuteServiceHandler.class})
	@Test
	public void testHandleWithMapInputsPost() {
		ExecuteServiceData edata = new ExecuteServiceData();
		edata.resourceId = "8";
		HashMap<String,String> dataInputs = new HashMap<String,String>();
		dataInputs.put("name","Marge");
		edata.setDataInputs(dataInputs);
		edata.setDataInput("");
		rm.method = "POST";
	
		when(template.postForEntity(Mockito.eq("http://localhost:8082/string/toUpper"),Mockito.any(MultiValueMap.class),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getResourceById("8")).thenReturn(rm);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ExecuteServiceHandler handler = new ExecuteServiceHandler(mockMongo,props,logger);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
		
	}
	
	@PrepareForTest({ExecuteServiceHandler.class})
	@Test
	public void testHandleWithMapInputsGet() {
		ExecuteServiceData edata = new ExecuteServiceData();
		edata.resourceId = "8";
		HashMap<String,String> dataInputs = new HashMap<String,String>();
		dataInputs.put("name","Marge");
		edata.setDataInputs(dataInputs);
		edata.setDataInput("");
		rm.method = "Get";
	
		when(template.getForEntity(Mockito.eq("http://localhost:8082/string/toUpper?name=Marge"),Mockito.eq(String.class),Mockito.any(MultiValueMap.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getResourceById("8")).thenReturn(rm);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ExecuteServiceHandler handler = new ExecuteServiceHandler(mockMongo,props,logger);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
		
	}
	@PrepareForTest({ExecuteServiceHandler.class})
	@Test
	public void testHandleWithMapInputPost() {
		ExecuteServiceData edata = new ExecuteServiceData();
		edata.resourceId = "8";
		HashMap<String,String> dataInputs = new HashMap<String,String>();
		edata.setDataInputs(dataInputs);
		edata.setDataInput("{\"name\":\"MovieQuoteWelcome\"}");
		rm.method = "POST";
	
		when(template.postForEntity(Mockito.eq("http://localhost:8082/string/toUpper"),Mockito.any(HttpEntity.class),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testHandleWithMapInputPost",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getResourceById("8")).thenReturn(rm);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ExecuteServiceHandler handler = new ExecuteServiceHandler(mockMongo,props,logger);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testHandleWithMapInputPost"));
	}
	@PrepareForTest({ExecuteServiceHandler.class})
	@Test
	public void testHandleWithMapInputGet() {
		ExecuteServiceData edata = new ExecuteServiceData();
		edata.resourceId = "8";
		HashMap<String,String> dataInputs = new HashMap<String,String>();
		
		edata.setDataInputs(dataInputs);
		edata.setDataInput("name=MovieQuoteWelcome");
		rm.method = "Get";
	
		when(template.getForEntity("http://localhost:8082/string/toUpper?name=MovieQuoteWelcome",String.class)).thenReturn(new ResponseEntity<String>("testHandleWithMapInputGet",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getResourceById("8")).thenReturn(rm);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ExecuteServiceHandler handler = new ExecuteServiceHandler(mockMongo,props,logger);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testHandleWithMapInputGet"));
		
	}

}
