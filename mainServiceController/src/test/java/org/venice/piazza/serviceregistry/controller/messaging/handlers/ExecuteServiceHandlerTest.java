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
import model.service.metadata.Format;
import model.service.metadata.MetadataType;
import model.service.metadata.ParamDataItem;
import model.service.metadata.Service;
import util.PiazzaLogger;



@RunWith(PowerMockRunner.class)
public class ExecuteServiceHandlerTest {
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
		rm.url = "http://localhost:8085/string/toUpper";
		rm.method = "POST";
		//rm.requestMimeType = "application/json";
		rm.id="Cheese";
		service = new Service();
		service.setResourceMetadata(rm);
		service.setId("8");
		ParamDataItem pitem = new ParamDataItem();
		DataType dataType1 = new URLParameterDataType();
		pitem.setDataType(dataType1);
		pitem.setName("aString");
		pitem.setMinOccurs(1);
		pitem.setMaxOccurs(1);
		List<ParamDataItem> inputs = new ArrayList<ParamDataItem>();
		inputs.add(pitem);
		service.setInputs(inputs);
		ParamDataItem output1 = new ParamDataItem();
		TextDataType dataType3 = new TextDataType();
		dataType3.mimeType = "appliction/json";
		output1.setDataType(dataType3);
		output1.setMaxOccurs(1);
		output1.setMinOccurs(1);
		output1.setName("Upper Case message");
		
		Format format1 = new Format();
		format1.setMimeType("application/json");
		List<Format> formats1 = new ArrayList<Format>();
		formats1.add(format1);
		output1.setFormats(formats1);
		MetadataType outMetadata = new MetadataType();
		outMetadata.setTitle("Upper Case Text");
		outMetadata.setAbout("ConvertToUpperCase");
		output1.setMetadata(outMetadata);
		
		List<ParamDataItem> outputs = new ArrayList<ParamDataItem>();
		outputs.add(output1);
		service.setOutputs(outputs);
    	
    }
	@PrepareForTest({ExecuteServiceHandler.class})
	//@Test
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
		
		
		List<ParamDataItem> inputs = new ArrayList<ParamDataItem>();
		service.setInputs(inputs);
		URI uri = URI.create("http://localhost:8085//string/toUpper");
		when(template.postForEntity(Mockito.eq(uri),Mockito.any(Object.class),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getServiceById("8")).thenReturn(service);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ExecuteServiceHandler handler = new ExecuteServiceHandler(mockMongo,props,logger);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
	}
	
	//@PrepareForTest({ExecuteServiceHandler.class})
	//@Test
	public void testHandleWithMapInputsPost() {
		ExecuteServiceData edata = new ExecuteServiceData();
		edata.setServiceId("8");
		ParamDataItem pitem = new ParamDataItem();
		TextDataType tdt = new TextDataType();
		pitem.setDataType(tdt);
		pitem.setName("name");
		List<ParamDataItem> inputs = new ArrayList<ParamDataItem>();
		inputs.add(pitem);
		service.setInputs(inputs);
		HashMap<String,DataType> dataInputs = new HashMap<String,DataType>();
		
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
		edata.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060f");
		ParamDataItem pitem = new ParamDataItem();
		URLParameterDataType urlPType = new URLParameterDataType();
		pitem.setDataType(urlPType);
		pitem.setName("aString");
		List<ParamDataItem> inputs = new ArrayList<ParamDataItem>();
		inputs.add(pitem);
		service.setInputs(inputs);
		HashMap<String,DataType> dataInputs = new HashMap<String,DataType>();
		TextDataType tdt = new TextDataType();
		tdt.content = "The rain in Spain";
		dataInputs.put("aString",tdt);
		edata.setDataInputs(dataInputs);
		ObjectMapper mapper = new ObjectMapper();
		try {
			String tsvc = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(edata);
			System.out.println(tsvc);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rm.method = "GET";
	    URI uri = URI.create("http://localhost:8085/string/toUpper?aString=The%20rain%20in%20Spain");
		when(template.getForEntity(Mockito.eq(uri),Mockito.eq(String.class))).thenReturn(new ResponseEntity<String>("testExecuteService",HttpStatus.FOUND));
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.getServiceById("a842aae2-bd74-4c4b-9a65-c45e8cd9060f")).thenReturn(service);
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		ExecuteServiceHandler handler = new ExecuteServiceHandler(mockMongo,props,logger);
		ResponseEntity<String> retVal = handler.handle(edata);
	    assertTrue(retVal.getBody().contains("testExecuteService"));
		
	}
	
	
	
	
}
