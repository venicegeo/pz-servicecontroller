package org.venice.piazza.serviceregistry.controller.messaging.handlers;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.data.DataType;
import model.data.type.TextDataType;
import model.data.type.URLParameterDataType;
import model.job.metadata.ResourceMetadata;
import model.service.metadata.Format;
import model.service.metadata.MetadataType;
import model.service.metadata.ParamDataItem;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;

@RunWith(PowerMockRunner.class)
public class RegistryServiceHandlerTest {
	RestTemplate template = null;
	Service service = null;
	ResourceMetadata rm  = null;
	UUIDFactory uuidFactory = null;
	@Before
	public void setup() {
	    uuidFactory = mock(UUIDFactory.class);
	    when(uuidFactory.getUUID()).thenReturn("NoDoz");
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
		
		//DEBUGGING
		ObjectMapper mapper = new ObjectMapper();
		try {
			String jsonReq = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(service);
			System.out.println(jsonReq);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@PrepareForTest({RegisterServiceHandler.class})
	@Test
	public void testHandleWithData() {
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.save(service)).thenReturn("8");
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		//when(props.getUuidservicehost().thenReturn("Nothing");
		PiazzaLogger logger = mock(PiazzaLogger.class);
		RegisterServiceHandler handler = new RegisterServiceHandler(mockMongo,props,logger,uuidFactory);
        String retVal = handler.handle(service);
        assertTrue(retVal.contains("8"));
        assertTrue(service.getId().contains("NoDoz"));
		
	}
}