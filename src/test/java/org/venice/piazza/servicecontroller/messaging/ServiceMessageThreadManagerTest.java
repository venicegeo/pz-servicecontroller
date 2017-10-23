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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.venice.piazza.servicecontroller.data.accessor.DatabaseAccessor;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.KafkaClientFactory;
import model.job.metadata.ResourceMetadata;
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
	private DatabaseAccessor accessorMock;
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
		service.setMethod( "POST");
		service.setResourceMetadata(rm);
		service.setUrl("http://localhost:8082/string/toUpper");
		MockitoAnnotations.initMocks(this);	
		
		// Inject test variables for kafka
		ReflectionTestUtils.setField(smtManager, "KAFKA_HOSTS", "localhost:9092");
		ReflectionTestUtils.setField(smtManager, "KAFKA_GROUP", "test-sc");
		ReflectionTestUtils.setField(smtManager, "SPACE", "unittest");
    }
	
	@Test
	/**
	 * Test Initialization
	 */
	public void testInitialization() {

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
	public void testPollingClosedConnection() {
		
		final ServiceMessageThreadManager smtmMock = Mockito.spy (smtManager);
		try {
			Mockito.doReturn(new AtomicBoolean(true)).when(smtmMock).makeAtomicBoolean();
			smtmMock.pollServiceJobs();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	/**
	 * Test Abort Polling losed Connection
	 */
	public void testAbortPollingClosedConnection() {
		
		final ServiceMessageThreadManager smtmMock = Mockito.spy (smtManager);
		try {
			Mockito.doReturn(new AtomicBoolean(true)).when(smtmMock).makeAtomicBoolean();
			smtmMock.pollAbortServiceJobs();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	@Test
	/**
	 * Test Polling
	 */
	public void testPollingNoConsumerRecords() {
		
		final ServiceMessageThreadManager smtmMock = Mockito.spy (smtManager);
		try {
			smtmMock.pollServiceJobs();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	/**
	 * Test polling of consumer records when nothing is returned
	 */
	@Test
	public void testPolling() {
		// Mock
		Mockito.doNothing().when(consumerMock).subscribe(Mockito.anyList());
		
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<String, String>(null);
		Mockito.when(consumerMock.poll(Mockito.anyLong())).thenReturn(consumerRecords);
		smtManager.pollServiceJobs();

    }

	static void setFinalStatic(Field field, Object newValue) throws Exception {
	        field.setAccessible(true);
	        Field modifiersField = Field.class.getDeclaredField("modifiers");
	        modifiersField.setAccessible(true);
	        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
	        
	        field.set(null, newValue);
	    
	}
	
}
