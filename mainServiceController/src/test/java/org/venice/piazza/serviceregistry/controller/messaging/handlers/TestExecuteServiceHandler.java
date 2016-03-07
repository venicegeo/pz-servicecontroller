package org.venice.piazza.serviceregistry.controller.messaging.handlers;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.data.type.TextDataType;
import model.job.metadata.ExecuteServiceData;
import model.job.metadata.InputType;
import model.job.metadata.ParamDataItem;
import model.job.metadata.ResourceMetadata;
import model.job.metadata.Service;
import util.PiazzaLogger;



@RunWith(PowerMockRunner.class)
public class TestExecuteServiceHandler {
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
		rm.url = "http://localhost:8082/string/toUpper";
		rm.method = "POST";
		//rm.requestMimeType = "application/json";
		rm.id="Cheese";
		service = new Service();
		service.setResourceMetadata(rm);
		service.setMimeType("application/json");
    	
    }
	@PrepareForTest({ExecuteServiceHandler.class})
	@Test
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
		edata.setServiceId("8");
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
		edata.setServiceId("8");
		ParamDataItem pitem = new ParamDataItem();
		pitem.setInputType(InputType.ComplexData);
		pitem.setName("name");
		List<ParamDataItem> inputs = new ArrayList<ParamDataItem>();
		inputs.add(pitem);
		service.setInputs(inputs);
		HashMap<String,Object> dataInputs = new HashMap<String,Object>();
		TextDataType tdt = new TextDataType();
		tdt.content = "My name is Marge";
		dataInputs.put("name",tdt);
		edata.setDataInputs(dataInputs);
		rm.method = "POST";
	    URI uri = URI.create("http://localhost:8082/string/toUpper");
		when(template.postForEntity(Mockito.eq(uri),Mockito.any(Object.class),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getServiceById("8")).thenReturn(service);
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
		edata.setServiceId("8");
		ParamDataItem pitem = new ParamDataItem();
		pitem.setInputType(InputType.URLParameter);
		pitem.setName("name");
		List<ParamDataItem> inputs = new ArrayList<ParamDataItem>();
		inputs.add(pitem);
		service.setInputs(inputs);
		HashMap<String,Object> dataInputs = new HashMap<String,Object>();
		TextDataType tdt = new TextDataType();
		tdt.content = "Marge";
		dataInputs.put("name",tdt);
		edata.setDataInputs(dataInputs);
		rm.method = "GET";
	    URI uri = URI.create("http://localhost:8082/string/toUpper?name=Marge");
		when(template.getForEntity(Mockito.eq(uri),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getServiceById("8")).thenReturn(service);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ExecuteServiceHandler handler = new ExecuteServiceHandler(mockMongo,props,logger);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
		
	}
	
	
	
	
}
