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

import javax.annotation.PostConstruct;

import org.apache.commons.lang.BooleanUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.controller.ServiceController;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.job.type.ExecuteServiceJob;
import model.status.StatusUpdate;

/**
 * Functionality for Task-Managed Services.
 * <p>
 * Task-Managed Services use Piazza Service Controller as a way to manage their job tasking/queueing. In the case of a
 * Task-Managed Service, Piazza makes no calls externally to that registered Service. Instead, that registered Service
 * is given a Jobs Queue in Piazza, that is populated with Service Execution Jobs that come in through Piazza. That
 * external Service is then responsible for pulling Jobs off of its Jobs queue in order to do the work.
 * </p>
 * <p>
 * This functionality allows for external registered Services to have an easy, 80% solution for task/queueing of Jobs in
 * an entirely RESTful manner, where Piazza handles the persistence and messaging of these Jobs.
 * </p>
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class ServiceTaskManager {
	@Value("${SPACE}")
	private String SPACE;
	@Value("${vcap.services.pz-kafka.credentials.host}")
	private String KAFKA_HOST;
	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private MongoAccessor mongoAccessor;

	private Producer<String, String> producer;
	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceTaskManager.class);

	@PostConstruct
	public void initialize() {
		String kafkaHost = KAFKA_HOST.split(":")[0];
		String kafkaPort = KAFKA_HOST.split(":")[1];
		producer = KafkaClientFactory.getProducer(kafkaHost, kafkaPort);
	}

	/**
	 * Creates a Service Queue for a newly registered Job.
	 * 
	 * @param serviceId
	 *            The Id of the Service
	 */
	public void createServiceQueue(String serviceId) {
		mongoAccessor.createServiceQueue(new ServiceQueue(serviceId));
	}

	/**
	 * Adds a Job to the Service's queue.
	 * 
	 * @param job
	 *            The Job to be executed. This information contains the serviceId, which is used to lookup the
	 *            appropriate Service Queue.
	 */
	public void addJobToQueue(ExecuteServiceJob job) {
		// Add the Job to the Jobs queue
		ServiceJob serviceJob = new ServiceJob(job.getJobId());
		mongoAccessor.addJobToServiceQueue(serviceJob);
		// Update the Job Status as Pending to Kafka
		StatusUpdate statusUpdate = new StatusUpdate();
		statusUpdate.setStatus(StatusUpdate.STATUS_PENDING);
		ProducerRecord<String, String> statusUpdateRecord;
		try {
			statusUpdateRecord = new ProducerRecord<String, String>(String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE),
					job.getJobId(), objectMapper.writeValueAsString(statusUpdate));
			producer.send(statusUpdateRecord);
		} catch (JsonProcessingException exception) {
			LOGGER.error("Error Sending Pending Job Status to Job Manager: " + exception.getMessage(), exception);
		}
	}

	/**
	 * Pulls the next waiting Job off of the Jobs queue and returns it.
	 * 
	 * @param serviceId
	 *            The ID of the Service whose Queue to pull a Job from
	 * @return The Job information
	 */
	public ExecuteServiceJob getNextJobFromQueue(String serviceId) {
		// Pull the Job off of the queue.
		ServiceJob serviceJob = mongoAccessor.getNextJobInServiceQueue(serviceId);
		String jobId = serviceJob.getJobId();
		// Read the Jobs collection for the full Job Details

		// Update the Job Status as Running

		// Return the Job
		return null;
	}
}
