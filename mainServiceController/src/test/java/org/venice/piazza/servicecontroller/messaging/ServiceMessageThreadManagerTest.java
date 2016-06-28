/*******************************************************************************
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.venice.piazza.servicecontroller.messaging;
import static org.hamcrest.CoreMatchers.instanceOf;
/**
 * Class of unit tests to test the deletion of services
 * @author mlynum
 */
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.mongojack.JacksonDBCollection;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.controller.ServiceController;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.DeleteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.DescribeServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ListServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.SearchServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.UpdateServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import org.venice.piazza.servicecontroller.util.TestUtilities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;

import messaging.job.KafkaClientFactory;
import model.data.DataType;
import model.data.type.BodyDataType;
import model.job.metadata.ResourceMetadata;
import model.job.type.RegisterServiceJob;
import model.request.PiazzaJobRequest;
import model.response.ErrorResponse;
import model.response.Pagination;
import model.response.PiazzaResponse;
import model.response.ServiceListResponse;
import model.response.ServiceResponse;
import model.response.ServiceIdResponse;
import model.response.SuccessResponse;
import model.service.SearchCriteria;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import util.PiazzaLogger;
@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaClientFactory.class)
public class ServiceMessageThreadManagerTest {
	@InjectMocks
	private ServiceMessageThreadManager smtManager;
	
	@Mock 
	private PiazzaLogger loggerMock;
	
	@Mock
	private MongoAccessor accessorMock;
	
	@Mock
	private Service serviceMock;
	
	@Mock
	private ObjectMapper omMock;
	@Mock
	private KafkaProducer<String, String> producerMock;
	@Mock
	private KafkaConsumer<String, String> consumerMock;
	
	@Mock
	private KafkaClientFactory kcFactoryMock;
	
	@Mock
	private CoreServiceProperties propertiesMock;

	
	ResourceMetadata rm = null;
	Service service = null;
	Service movieService = null;
	Service convertService = null;
	
	@Before
	/** 
	 * Called for each test setup
	 */
    public void setup() {
        // Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toUpper Params";
		rm.description = "Service to convert string to uppercase";

		service = new Service();
		service.method = "POST";
		service.setResourceMetadata(rm);
		service.setUrl("http://localhost:8082/string/toUpper");
		MockitoAnnotations.initMocks(this);			

    }
	
	@Test
	/**
	 * Test Initialization
	 */
	public void testInitialization() {
		final ServiceMessageThreadManager smtmMock = Mockito.spy (smtManager);

		Mockito.when(propertiesMock.getKafkaGroup()).thenReturn("ServiceController Group");
		Mockito.when(propertiesMock.getKafkaHost()).thenReturn("localhost:8087");		
		PowerMockito.mockStatic(KafkaClientFactory.class);
		PowerMockito.when(KafkaClientFactory.getProducer("localhost", "8087")).thenReturn(producerMock);
		PowerMockito.when(KafkaClientFactory.getConsumer("localhost", "8087", "ServiceController Group")).thenReturn(consumerMock);
		try {
			
			smtManager.initialize();
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	@Test
	/**
	 * Test Polling
	 */
	public void testPolling() {
		
		final ServiceMessageThreadManager esMock = Mockito.spy (smtManager);

		
	}

}
