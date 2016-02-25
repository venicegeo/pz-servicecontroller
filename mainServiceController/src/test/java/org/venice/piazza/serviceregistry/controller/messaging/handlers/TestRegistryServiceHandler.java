package org.venice.piazza.serviceregistry.controller.messaging.handlers;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.ArrayList;

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

import model.job.metadata.ResourceMetadata;
import model.resource.UUID;
import util.PiazzaLogger;
import util.UUIDFactory;

@RunWith(PowerMockRunner.class)
public class TestRegistryServiceHandler {
	RestTemplate template = null;
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
		rm.requestMimeType = "application";
	}
	
	@PrepareForTest({RegisterServiceHandler.class})
	@Test
	public void testHandleWithData() {
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.save(rm)).thenReturn("8");
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		when(props.getUuidservice()).thenReturn("Nothing");
		PiazzaLogger logger = mock(PiazzaLogger.class);
		RegisterServiceHandler handler = new RegisterServiceHandler(mockMongo,props,logger,uuidFactory);
        String retVal = handler.handle(rm);
        assertTrue(retVal.contains("8"));
        assertTrue(rm.id.contains("NoDoz"));
		
	}
}