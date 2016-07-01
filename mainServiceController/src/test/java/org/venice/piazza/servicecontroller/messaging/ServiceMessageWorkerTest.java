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

import static org.junit.Assert.*;
import static org.mockito.Matchers.isA;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
import model.job.Job;
import model.job.metadata.ResourceMetadata;
import model.job.type.ExecuteServiceJob;
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
import util.UUIDFactory;

/**
 * Class of unit tests to test the deletion of services
 * @author mlynum
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaClientFactory.class)

public class ServiceMessageWorkerTest {
	

	@InjectMocks
    private ServiceMessageWorker smWorkerMock;
	
	@Mock
	private RegisterServiceHandler rsHandlerMock;
	
	@Mock
	private ExecuteServiceHandler esHandlerMock;
	
	@Mock
	private DescribeServiceHandler dsHandlerMock;
	
	@Mock
	private UpdateServiceHandler usHandlerMock;
	
	@Mock
	private ListServiceHandler lsHandlerMock;
	
	@Mock
	private DeleteServiceHandler dlHandlerMock;
	
	@Mock
	private SearchServiceHandler ssHandlerMock;
	
	@Mock
	private CoreServiceProperties coreServicePropMock;
	
	@Mock 
	private PiazzaLogger loggerMock;
	
	@Mock
	private UUIDFactory uuidFactoryMock;
	
	@Mock
	private Producer<String, String> producerMock;
	
	private Job job;
	private ExecuteServiceJob esJob;
	ResourceMetadata rm = null;
	Service service = null;
	
	ConsumerRecord<String, String> kafkaMessage;
	

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
		
		// Create the executeService Job
		ExecuteServiceJob esJob = new ExecuteServiceJob();
		// Setup executeServiceData
		ExecuteServiceData edata = new ExecuteServiceData();
		String serviceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060f";
		edata.setServiceId(serviceId);	
		// Now tie the data to the job
		esJob.data = edata;
		job = new Job();
		job.jobType = esJob;
		
		// Mock the Kafka response that Producers will send. This will always
				// return a Future that completes immediately and simply returns true.
				Mockito. when(producerMock.send(isA(ProducerRecord.class))).thenAnswer(new Answer<Future<Boolean>>() {
					@Override
					public Future<Boolean> answer(InvocationOnMock invocation) throws Throwable {
						Future<Boolean> future = Mockito.mock(FutureTask.class);
						Mockito.when(future.isDone()).thenReturn(true);
						Mockito.when(future.get()).thenReturn(true);
						return future;
					}
		});
				
				
		MockitoAnnotations.initMocks(this);			

    }
		
	@Test
	/**
	 * Test an invalid kafka message being received
	 */
	public void testWorkerInvalidPayload() {
		try {
			// Test exception by sending an invalid ConsumerMessage
			ConsumerRecord<String, String> kafkaMessage = new ConsumerRecord<String, String>("Test", 0, 0, "123456",
					"INVALID_JSON");
			Future<String> workerFuture = smWorkerMock.run(kafkaMessage, producerMock, job, null);
			assertTrue(workerFuture.get() != null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

}
