package org.venice.piazza.serviceregistry.controller.messaging.handlers;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.ArrayList;

import model.job.metadata.ResourceMetadata;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.CoreServiceProperties;
import org.venice.piazza.servicecontroller.data.model.UUID;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreLogger;

@RunWith(PowerMockRunner.class)
public class TestRegistryServiceHandler {
	RestTemplate template = null;
	ResourceMetadata rm  = null;
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
		rm.requestMimeType = "application";
	}
	@PrepareForTest({RegisterServiceHandler.class})
	@Test
	public void testHandleWithNoUUIDBodyData() {
		
		UUID uuid = mock(UUID.class);
		when(uuid.getData()).thenReturn(null);
		ResponseEntity<UUID> uuidEntity = new ResponseEntity<UUID>(uuid,HttpStatus.NOT_FOUND);
		String url = "http://Nothing";
		when(template.postForEntity(url, null, UUID.class)).thenReturn(uuidEntity);
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.save(rm)).thenReturn("8");
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		when(props.getUuidservice()).thenReturn("Nothing");
		CoreLogger logger = mock(CoreLogger.class);
		RegisterServiceHandler handler = new RegisterServiceHandler(mockMongo,props,logger);
        String retVal = handler.handle(rm);
        assertTrue(retVal.contains("8"));
		
		
		
	}
	
	@PrepareForTest({RegisterServiceHandler.class})
	@Test
	public void testHandleWithNoUUIDBody() {
		
		ResponseEntity<UUID> uuidEntity = new ResponseEntity<UUID>(HttpStatus.NOT_FOUND);
		String url = "http://Nothing";
		when(template.postForEntity(url, null, UUID.class)).thenReturn(uuidEntity);
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.save(rm)).thenReturn("8");
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		when(props.getUuidservice()).thenReturn("Nothing");
		CoreLogger logger = mock(CoreLogger.class);
		RegisterServiceHandler handler = new RegisterServiceHandler(mockMongo,props,logger);
        String retVal = handler.handle(rm);
        assertTrue(retVal.contains("8"));
		
		
		
	}
	@PrepareForTest({RegisterServiceHandler.class})
	@Test
	public void testHandleWithData() {
		ArrayList<String> bodyData = new ArrayList<String>();
		bodyData.add("resourceId");
		UUID uuid = mock(UUID.class);
		when(uuid.getData()).thenReturn(bodyData);
		ResponseEntity<UUID> uuidEntity = new ResponseEntity<UUID>(uuid,HttpStatus.NOT_FOUND);
		String url = "http://Nothing";
		when(template.postForEntity(url, null, UUID.class)).thenReturn(uuidEntity);
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.save(rm)).thenReturn("8");
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		when(props.getUuidservice()).thenReturn("Nothing");
		CoreLogger logger = mock(CoreLogger.class);
		RegisterServiceHandler handler = new RegisterServiceHandler(mockMongo,props,logger);
        String retVal = handler.handle(rm);
        assertTrue(retVal.contains("8"));
        assertTrue(rm.resourceId.contains("resourceId"));
		
	}
}
