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
		rm.method = "POST";
		service = new Service();
		service.setResourceMetadata(rm);
		service.setServiceId("8");
		ParamDataItem pitem = new ParamDataItem();
		DataType dataType1 = new URLParameterDataType();
		pitem.setDataType(dataType1);
		pitem.setName("aString");
		pitem.setMinOccurs(1);
		pitem.setMaxOccurs(1);
		
		
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
        assertTrue(retVal.contains("NoDoz"));
        assertTrue(service.getServiceId().contains("NoDoz"));
		
	}
}