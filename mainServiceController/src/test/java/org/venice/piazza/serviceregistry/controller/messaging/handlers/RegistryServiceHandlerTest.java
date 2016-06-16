package org.venice.piazza.serviceregistry.controller.messaging.handlers;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;


import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.data.DataType;
import model.data.type.URLParameterDataType;
import model.job.metadata.ResourceMetadata;
import model.response.ServiceResponse;

import model.service.metadata.ParamDataItem;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;

@RunWith(PowerMockRunner.class)
public class RegistryServiceHandlerTest {
	
	@Mock
	private RegisterServiceHandler rsHandler;
	
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
		service = new Service();
		service.method = "POST";
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
	@Ignore
	public void testHandleWithData() {
		ElasticSearchAccessor mockElasticAccessor = mock(ElasticSearchAccessor.class);
		when(mockElasticAccessor.save(service)).thenReturn(new ServiceResponse());
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.save(service)).thenReturn("8");
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		//when(props.getUuidservicehost().thenReturn("Nothing");
		PiazzaLogger logger = mock(PiazzaLogger.class);
        String retVal = rsHandler.handle(service);
        assertTrue(retVal.contains("NoDoz"));
        assertTrue(service.getServiceId().contains("NoDoz"));
		
	}
	
	@PrepareForTest({RegisterServiceHandler.class})
	@Test
	public void testHandleWithoutData() {
		ElasticSearchAccessor mockElasticAccessor = mock(ElasticSearchAccessor.class);
		when(mockElasticAccessor.save(service)).thenReturn(new ServiceResponse());
		MongoAccessor mockMongo = mock(MongoAccessor.class);
		when(mockMongo.save(service)).thenReturn("8");
		CoreServiceProperties props = mock(CoreServiceProperties.class);
		PiazzaLogger logger = mock(PiazzaLogger.class);
		Service nullService = null;
        String retVal = rsHandler.handle(nullService);
        assertNull(retVal);
	}
	
	
}