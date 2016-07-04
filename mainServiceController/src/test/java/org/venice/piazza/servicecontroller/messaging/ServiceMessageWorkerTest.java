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
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyMap;
//import static org.powermock.api.mockito.PowerMockito.when;

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
import model.data.DataResource;
import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.GeoJsonDataType;
import model.data.type.TextDataType;
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
import model.status.StatusUpdate;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Class of unit tests to test the deletion of services
 * @author mlynum
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaClientFactory.class})

public class ServiceMessageWorkerTest {
	
	
	@Mock
	private PiazzaLogger loggerMock;
	
	
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
	private UUIDFactory uuidFactoryMock;
	
	@Mock
	private Producer<String, String> producerMock;
	
	@Mock
	private ObjectMapper omMock;
	
	private Job validJob;
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
		// Setup valid data
		ExecuteServiceData edata = new ExecuteServiceData();
		String serviceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060f";
		edata.setServiceId(serviceId);	
		TextDataType dataType  = new TextDataType();
		dataType.mimeType = "application/json";
		List <DataType> dataTypes = new ArrayList <>();
		dataTypes.add(dataType);
		edata.setDataOutput(dataTypes);
		// Now tie the data to the job
		esJob.data = edata;		
		validJob = new Job();
		validJob.jobId = "b842aae2-ed70-5c4b-9a65-c45e8cd9060g";
		validJob.jobType = esJob;
		
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
	 * Test a null job being passed
	 */
	public void testNullob() {
		try {
			// Test exception by sending an invalid ConsumerMessage
			ConsumerRecord<String, String> kafkaMessage = new ConsumerRecord<String, String>("Test", 0, 0, "123456",
					"INVALID_JSON");
			Future<String> workerFuture = smWorkerMock.run(kafkaMessage, producerMock, null, null);
			assertTrue(workerFuture.get() != null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		
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
			Future<String> workerFuture = smWorkerMock.run(kafkaMessage, producerMock, createInvalidJobWithoutOuptut(), null);
			assertTrue(workerFuture.get() != null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	@Test
	/**
	 * Test an invalid kafka message being received
	 */
	public void testWorkerValidPayload() {
		try {
			
		        ServiceMessageWorker spy = Mockito.spy(smWorkerMock);
				
				DataResource data = new DataResource();
		        TextDataType newDataType = new TextDataType();
				newDataType.content = "This is a result";
				data.dataType = newDataType;
				data.dataId="b842aae2-ed70-5c4b-9a65-c45e8cd9060g";
				ObjectMapper om = new ObjectMapper();
				
				String executeResponse = om.writeValueAsString(data);
		        ResponseEntity<String>  response =  new ResponseEntity<>(executeResponse, HttpStatus.OK);
		        
		        ExecuteServiceJob jobItem = (ExecuteServiceJob) validJob.jobType;
				ExecuteServiceData esData = jobItem.data;
				Mockito.when(esHandlerMock.handle(jobItem)).thenReturn(response);
				Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Mockito.anyString());
		        
			// Test valid Payload
			ConsumerRecord<String, String> kafkaMessage = new ConsumerRecord<String, String>("Test", 0, 0, "123456",
					"VALID");
			Future<String> workerFuture = spy.run(kafkaMessage, producerMock, validJob, null);
			assertTrue(workerFuture.get() != null);
		
		
			// Now try with GeoJson
			ExecuteServiceJob esJob = new ExecuteServiceJob();
			ExecuteServiceData edata = new ExecuteServiceData();
			String serviceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060f";
			edata.setServiceId(serviceId);	
			GeoJsonDataType dataType  = new GeoJsonDataType();
			dataType.mimeType = "application/vnd.geo+json";
			List <DataType> dataTypes = new ArrayList <>();
			dataTypes.add(dataType);
			edata.setDataOutput(dataTypes);
			// Now tie the data to the job
			esJob.data = edata;		
			validJob = new Job();
			validJob.jobId = "b842aae2-ed70-5c4b-9a65-c45e8cd9060g";
			validJob.jobType = esJob;
		
		     om = new ObjectMapper();
		
			// Test valid Payload for GeoJSON
			kafkaMessage = new ConsumerRecord<String, String>("Test", 0, 0, "123456",
				"VALID");
			workerFuture = spy.run(kafkaMessage, producerMock, validJob, null);
			assertTrue(workerFuture.get() != null);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	@Test
	/**
	 * Test what happens if a Null is returned from handling the execute
	 */
	public void testNullHandleResult() {
		try {
			
		        ServiceMessageWorker spy = Mockito.spy(smWorkerMock);
				
				DataResource data = new DataResource();
		        TextDataType newDataType = new TextDataType();
				newDataType.content = "This is a result";
				data.dataType = newDataType;
				data.dataId="b842aae2-ed70-5c4b-9a65-c45e8cd9060g";
				ObjectMapper om = new ObjectMapper();
				
				String executeResponse = om.writeValueAsString(data);
		        ResponseEntity<String>  response =  new ResponseEntity<>(executeResponse, HttpStatus.OK);
		        
		        ExecuteServiceJob jobItem = (ExecuteServiceJob) validJob.jobType;
				ExecuteServiceData esData = jobItem.data;
				// What happens if the handled executeservice returns a null
				Mockito.when(esHandlerMock.handle(jobItem)).thenReturn(null);
				Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Mockito.anyString());
		        
			// Test valid Payload
			ConsumerRecord<String, String> kafkaMessage = new ConsumerRecord<String, String>("Test", 0, 0, "123456",
					"VALID");
			Future<String> workerFuture = spy.run(kafkaMessage, producerMock, validJob, null);
			assertTrue(workerFuture.get() != null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	/**
	 * Test an invalid kafka message being received
	 */
	@Test
	public void testBadHTTPResponseServiceResponse() {
		try {
			
		        ServiceMessageWorker spy = Mockito.spy(smWorkerMock);
				
				DataResource data = new DataResource();
		        TextDataType newDataType = new TextDataType();
				newDataType.content = "This is a result";
				data.dataType = newDataType;
				data.dataId="b842aae2-ed70-5c4b-9a65-c45e8cd9060g";
				ObjectMapper om = new ObjectMapper();
				
				String executeResponse = om.writeValueAsString(data);
		        ResponseEntity<String>  response =  new ResponseEntity<>(executeResponse, HttpStatus.BAD_REQUEST);
		        
		        ExecuteServiceJob jobItem = (ExecuteServiceJob) validJob.jobType;
				ExecuteServiceData esData = jobItem.data;
				Mockito.when(esHandlerMock.handle(jobItem)).thenReturn(response);
				Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Mockito.anyString());
		        
			// Test valid Payload
			ConsumerRecord<String, String> kafkaMessage = new ConsumerRecord<String, String>("Test", 0, 0, "123456",
					"VALID");
			Future<String> workerFuture = spy.run(kafkaMessage, producerMock, validJob, null);
			assertTrue(workerFuture.get() != null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	/**
	 * Test an invalid kafka message being received
	 */
	@Test
	public void testServicResponseObjectMapperError() {
		try {
			
		        ServiceMessageWorker spy = Mockito.spy(smWorkerMock);
				
				DataResource data = new DataResource();
		        TextDataType newDataType = new TextDataType();
				newDataType.content = "This is a result";
				data.dataType = newDataType;
				data.dataId="b842aae2-ed70-5c4b-9a65-c45e8cd9060g";
				ObjectMapper om = new ObjectMapper();
				
				String executeResponse = om.writeValueAsString(data);
		        ResponseEntity<String>  response =  new ResponseEntity<>(executeResponse, HttpStatus.OK);
		        
		        ExecuteServiceJob jobItem = (ExecuteServiceJob) validJob.jobType;
				ExecuteServiceData esData = jobItem.data;
				Mockito.when(esHandlerMock.handle(jobItem)).thenReturn(response);
				Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Mockito.anyString());
				Mockito.when(spy.makeNewObjectMapper()).thenReturn(omMock);
				Mockito.when(omMock.readValue(Mockito.anyString(), eq(DataResource.class))).thenReturn(null);
		        
			// Test valid Payload
			ConsumerRecord<String, String> kafkaMessage = new ConsumerRecord<String, String>("Test", 0, 0, "123456",
					"VALID");
			Future<String> workerFuture = spy.run(kafkaMessage, producerMock, validJob, null);
			assertTrue(workerFuture.get() != null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	@Test
	/**
	 * Test some other job that is received
	 */
	public void testInvalidJob() {
		try {
			
	        ServiceMessageWorker spy = Mockito.spy(smWorkerMock);
			RegisterServiceJob rsj = new RegisterServiceJob();
			rsj.data = service;
			validJob.jobType = rsj;
			
			Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Mockito.anyString());
		        
			// Test valid Payload
			ConsumerRecord<String, String> kafkaMessage = new ConsumerRecord<String, String>("Test", 0, 0, "123456",
					"VALID");
			Future<String> workerFuture = spy.run(kafkaMessage, producerMock, validJob, null);
			assertTrue(workerFuture.get() != null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	private Job createInvalidJobWithoutOuptut() {
		
		Job job = new Job();
		
		// Create the executeService Job
		ExecuteServiceJob esJob = new ExecuteServiceJob();
		// Setup valid data
		ExecuteServiceData edata = new ExecuteServiceData();
		String serviceId = "a842aae2-bd74-4c4b-9a65-c45e8cd9060f";
		edata.setServiceId(serviceId);	
		// Now tie the data to the job
		esJob.data = edata;		
		job = new Job();
		job.jobType = esJob;
		
		return job;
	}

}
