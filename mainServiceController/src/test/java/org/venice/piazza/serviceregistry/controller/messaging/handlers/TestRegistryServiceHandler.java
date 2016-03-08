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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.job.metadata.InputType;
import model.job.metadata.ParamDataItem;
import model.job.metadata.ResourceMetadata;
import model.job.metadata.Service;
import model.resource.UUID;
import util.PiazzaLogger;
import util.UUIDFactory;

@RunWith(PowerMockRunner.class)
public class TestRegistryServiceHandler {
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
		service.setMimeType("application/json");
		service.setId("8");
		ParamDataItem pitem = new ParamDataItem();
		pitem.setInputType(InputType.ComplexData);
		pitem.setName("name");
		List<ParamDataItem> inputs = new ArrayList<ParamDataItem>();
		inputs.add(pitem);
		service.setInputs(inputs);
		//rm.requestMimeType = "application";
	}
	
	@PrepareForTest({RegisterServiceHandler.class})
	@Test
	public void testHandleWithData() {
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.save(service)).thenReturn("8");
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		when(props.getUuidservice()).thenReturn("Nothing");
		PiazzaLogger logger = mock(PiazzaLogger.class);
		RegisterServiceHandler handler = new RegisterServiceHandler(mockMongo,props,logger,uuidFactory);
        String retVal = handler.handle(service);
        assertTrue(retVal.contains("8"));
        assertTrue(service.getId().contains("NoDoz"));
		
	}
}