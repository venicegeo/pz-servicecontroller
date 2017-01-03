/**
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
 **/
package org.venice.piazza.servicecontroller.taskmanaged;

import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;

import exception.InvalidInputException;
import model.job.type.ExecuteServiceJob;
import model.service.metadata.ExecuteServiceData;
import model.service.taskmanaged.ServiceJob;
import model.status.StatusUpdate;
import util.PiazzaLogger;

/**
 * Tests for Task Managed Service Queues
 * 
 * @author Patrick.Doody
 *
 */
public class TaskManagedTests {
	@Mock
	private ObjectMapper objectMapper;
	@Mock
	private MongoAccessor mongoAccessor;
	@Mock
	private PiazzaLogger piazzaLogger;
	@Mock
	private Producer<String, String> producer;

	@InjectMocks
	private ServiceTaskManager serviceTaskManager;

	/**
	 * Test initialization
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		ReflectionTestUtils.setField(serviceTaskManager, "KAFKA_HOST", "localhost:1234");
		ReflectionTestUtils.setField(serviceTaskManager, "SPACE", "UnitTest");
		ReflectionTestUtils.setField(serviceTaskManager, "TIMEOUT_LIMIT_COUNT", 5);
	}

	/**
	 * Tests adding a job to a Service's queue.
	 */
	@Test
	public void testAddJob() throws JsonProcessingException {
		// Mock Data
		ExecuteServiceJob job = new ExecuteServiceJob("job123");
		job.setData(new ExecuteServiceData());
		job.getData().setServiceId("service123");

		// Test, ensure no errors
		serviceTaskManager.addJobToQueue(job);

		// Test Exception handling, ensure exception is handled
		Mockito.when(objectMapper.writeValueAsString(Mockito.any())).thenThrow(new JsonMappingException("Oops"));
	}

	/**
	 * Tests updating Status for a Job, with a null Service found
	 */
	@Test(expected = InvalidInputException.class)
	public void testStatusUpdateError() throws MongoException, InvalidInputException {
		serviceTaskManager.processStatusUpdate("noServiceHere", "123456", new StatusUpdate());
	}

	/**
	 * Tests updating a Status
	 */
	@Test
	public void testStatusUpdate() throws JsonProcessingException, MongoException, InvalidInputException {
		// Mock
		StatusUpdate mockUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING);
		ServiceJob mockJob = new ServiceJob("job123", "service123");
		Mockito.when(mongoAccessor.getServiceJob(Mockito.eq("service123"), Mockito.eq("job123"))).thenReturn(mockJob);

		// Test - Kafka succeeds
		serviceTaskManager.processStatusUpdate("service123", "job123", mockUpdate);

		// Test - Final Status
		mockUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
		serviceTaskManager.processStatusUpdate("service123", "job123", mockUpdate);

		// Test - Kafka fails
		Mockito.when(objectMapper.writeValueAsString(Mockito.any())).thenThrow(new JsonMappingException("Oops"));
		serviceTaskManager.processStatusUpdate("service123", "job123", mockUpdate);
	}

	/**
	 * Tests pulling a Job off the queue for a service that doesn't exist; thus causing an exception.
	 */
	@Test(expected = ResourceAccessException.class)
	public void testGetJobError() {
		// Mock

		// Test

	}
}
