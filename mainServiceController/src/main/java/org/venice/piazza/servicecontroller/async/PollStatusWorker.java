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
package org.venice.piazza.servicecontroller.async;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.ServiceMessageWorker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.job.result.type.DataResult;
import model.job.result.type.ErrorResult;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * This Worker will make the direct REST request to that User Service, and update the Instance table based on the status
 * and poll time.
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class PollStatusWorker {
	@Value("${async.status.endpoint}")
	private String STATUS_ENDPOINT;
	@Value("${async.results.endpoint}")
	private String RESULTS_ENDPOINT;
	@Value("${async.delete.endpoint}")
	private String DELETE_ENDPOINT;
	@Value("${vcap.services.pz-kafka.credentials.host}")
	private String KAFKA_HOST_PORT;
	@Value("${SPACE}")
	private String SPACE;
	@Value("${async.status.error.limit}")
	private int STATUS_ERROR_LIMIT;

	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private ServiceMessageWorker serviceMessageWorker;
	@Autowired
	private UUIDFactory uuidFactory;

	private RestTemplate restTemplate = new RestTemplate();
	private Producer<String, String> producer;
	private ObjectMapper objectMapper = new ObjectMapper();

	@PostConstruct
	public void initialize() {
		String KAFKA_HOST = KAFKA_HOST_PORT.split(":")[0];
		String KAFKA_PORT = KAFKA_HOST_PORT.split(":")[1];
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
	}

	/**
	 * Polls for the Status of the Asynchronous Service Instance. This will update any status information in the Status
	 * table, and will also check if the Status is in a completed State. If a completed state is detected (success or
	 * fail) then it will initialize the logic to handle the result or error.
	 * 
	 * @param instance
	 */
	@Async
	public void pollStatus(AsyncServiceInstance instance) {
		// Get the Service, so we can fetch the URL
		Service service = accessor.getServiceById(instance.getServiceId());
		// Build the GET URL
		String url = String.format("%s/%s/%s", service.getUrl(), STATUS_ENDPOINT, instance.getInstanceId());
		// Poll
		try {
			// Get the Status of the job.
			StatusUpdate status = restTemplate.getForObject(url, StatusUpdate.class);

			// Act appropriately based on the status received
			if ((status.getStatus().equals(StatusUpdate.STATUS_PENDING)) || (status.getStatus().equals(StatusUpdate.STATUS_RUNNING))
					|| (status.getStatus().equals(StatusUpdate.STATUS_SUBMITTED))) {
				// If this service is not done, then mark the status and we'll poll again later.
				instance.setStatus(status);
				instance.setLastCheckedOn(new DateTime());
				accessor.updateAsyncServiceInstance(instance);
				// Route the current Job Status through Kafka.
				try {
					ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(
							String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), instance.getJobId(),
							objectMapper.writeValueAsString(status));
					producer.send(prodRecord);
				} catch (JsonProcessingException exception) {
					// The message could not be serialized. Record this.
					exception.printStackTrace();
					logger.log("Could not send Running Status Message to Job Manager. Error serializing Status: " + exception.getMessage(),
							PiazzaLogger.ERROR);
				}
			} else if (status.getStatus().equals(StatusUpdate.STATUS_SUCCESS)) {
				// Queue up a subsequent request to get the Result of the Instance
				processSuccessStatus(service, instance);
			} else if ((status.getStatus().equals(StatusUpdate.STATUS_ERROR)) || (status.getStatus().equals(StatusUpdate.STATUS_FAIL))
					|| (status.getStatus().equals(StatusUpdate.STATUS_CANCELLED))) {
				// Errors encountered. Report this and bubble it back up through the Job ID.
				String errorMessage = String.format("Instance %s reported back Status %s. ", instance.getInstanceId(), status.getStatus());
				if (status.getResult() instanceof ErrorResult) {
					// If we can parse any further details on the error, then do so here.
					ErrorResult errorResult = (ErrorResult) status.getResult();
					errorMessage = String.format("%s Details: %s, %s", errorMessage, errorResult.getMessage(), errorResult.getDetails());
				}
				logger.log(errorMessage, PiazzaLogger.ERROR);
				processErrorStatus(instance, status.getStatus(), errorMessage);
			}
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			updateFailureCount(instance);
			logger.log(String.format(
					"HTTP Error Status %s encountered for Service ID %s Instance %s under Job ID %s. The number of Errors has been incremented (%s)",
					exception.getStatusCode().toString(), instance.getServiceId(), instance.getInstanceId(), instance.getJobId(),
					instance.getNumberErrorResponses()), PiazzaLogger.WARNING);
		} catch (Exception exception) {
			updateFailureCount(instance);
			logger.log(String.format(
					"Unexpected Error %s encountered for Service ID %s Instance %s under Job ID %s. The number of Errors has been incremented (%s)",
					exception.getMessage(), instance.getServiceId(), instance.getInstanceId(), instance.getJobId(),
					instance.getNumberErrorResponses()), PiazzaLogger.WARNING);
		}
	}

	/**
	 * Updates the failure count for the Instance.
	 * 
	 * @param instance
	 *            The Instance.
	 */
	private void updateFailureCount(AsyncServiceInstance instance) {
		// Increment the failure count
		instance.setNumberErrorResponses(instance.getNumberErrorResponses() + 1);
		// Check if the Failure count is above the threshold. If so, then fail the job.
		if (instance.getNumberErrorResponses() > STATUS_ERROR_LIMIT) {
			// Failure threshold has been reached. Fail the job.
			String errorMessage = String.format(
					"Job ID %s for Service ID %s Instance ID %s has failed too many times during periodic Status Checks. This Job is being marked as a failure.",
					instance.getJobId(), instance.getServiceId(), instance.getInstanceId());
			logger.log(errorMessage, PiazzaLogger.ERROR);
			// Remove this from the Collection of tracked instance Jobs.
			accessor.deleteAsyncServiceInstance(instance.getJobId());
			// Send a Failure message back to the Job Manager via Kafka.
			processErrorStatus(instance, StatusUpdate.STATUS_ERROR, errorMessage);
		} else {
			// Update the Database that this instance has failed.
			accessor.updateAsyncServiceInstance(instance);
		}
	}

	/**
	 * Handles a successful Instance. This will make a call to the results endpoint to grab the results of the service.
	 * 
	 * @param service
	 *            The Service metadata information (used to grab URL, etc)
	 * @param instance
	 *            The User Service execution instance
	 */
	private void processSuccessStatus(Service service, AsyncServiceInstance instance) {
		// Log
		logger.log(String.format("Handling Successful status of Instance %s for Service %s under Job ID %s", instance.getInstanceId(),
				instance.getServiceId(), instance.getJobId()), PiazzaLogger.INFO);
		// Make a request to the results endpoint to get the results of the Service
		String url = String.format("%s/%s/%s", service.getUrl(), RESULTS_ENDPOINT, instance.getInstanceId());
		try {
			ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
			String dataId = uuidFactory.getUUID();
			// Get the Result of the Service
			DataResult result = serviceMessageWorker.processExecutionResult(instance.getOutputType(), producer, StatusUpdate.STATUS_SUCCESS,
					response, dataId);
			// Send the Completed Status to the Job Manager, including the Result
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
			statusUpdate.setResult(result);
			ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(instance.getJobId(), statusUpdate, SPACE);
			producer.send(prodRecord);
			// Remove this Instance from the Instance table
			accessor.deleteAsyncServiceInstance(instance.getJobId());
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			updateFailureCount(instance);
			logger.log(String.format(
					"Error fetching Service results: HTTP Error Status %s encountered for Service ID %s Instance %s under Job ID %s. The number of Errors has been incremented (%s)",
					exception.getStatusCode().toString(), instance.getServiceId(), instance.getInstanceId(), instance.getJobId(),
					instance.getNumberErrorResponses()), PiazzaLogger.WARNING);
		} catch (Exception exception) {
			updateFailureCount(instance);
			logger.log(String.format(
					"Unexpected Error fetching Service results: %s encountered for Service ID %s Instance %s under Job ID %s. The number of Errors has been incremented (%s)",
					exception.getMessage(), instance.getServiceId(), instance.getInstanceId(), instance.getJobId(),
					instance.getNumberErrorResponses()), PiazzaLogger.WARNING);
		}
	}

	/**
	 * Handles a non-success completed Status message.
	 * 
	 * @param instance
	 *            The service instance.
	 * @param serviceStatus
	 *            The StatusUpdate received from the external User Service
	 */
	private void processErrorStatus(AsyncServiceInstance instance, String status, String message) {
		// Remove the Instance from the Instance Table
		accessor.deleteAsyncServiceInstance(instance.getJobId());

		// Create a new Status Update to send to the Job Manager.
		StatusUpdate statusUpdate = new StatusUpdate();
		statusUpdate.setStatus(status);
		// Create the Message for the Error Result of the Status
		ErrorResult errorResult = new ErrorResult();
		errorResult.setMessage(message);
		statusUpdate.setResult(errorResult);

		// Send the Job Status through Kafka.
		try {
			ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(
					String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), instance.getJobId(),
					objectMapper.writeValueAsString(statusUpdate));
			producer.send(prodRecord);
		} catch (JsonProcessingException exception) {
			// The message could not be serialized. Record this.
			exception.printStackTrace();
			logger.log("Could not send Error Status to Job Manager. Error serializing Status: " + exception.getMessage(),
					PiazzaLogger.ERROR);
		}
	}

	/**
	 * Sends the cancellation status to the external User Service for the specified instance.
	 * 
	 * @param instance
	 *            The instance to be cancelled
	 */
	public void sendCancellationStatus(AsyncServiceInstance instance) {
		// Send the DELETE request to the external User Service
		Service service = accessor.getServiceById(instance.getServiceId());
		String url = String.format("%s/%s/%s", service.getUrl(), DELETE_ENDPOINT, instance.getInstanceId());
		try {
			restTemplate.delete(url);
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			// The cancellation sent back an error. Log it.
			logger.log(String.format(
					"Error Cancelling Service Instance on external User Service: HTTP Error Status %s encountered for Service ID %s Instance %s under Job ID %s. No subsequent calls will be made.",
					exception.getStatusCode().toString(), instance.getServiceId(), instance.getInstanceId(), instance.getJobId(),
					instance.getNumberErrorResponses()), PiazzaLogger.WARNING);
		}
		// Remove this from the Instance Table
		accessor.deleteAsyncServiceInstance(instance.getJobId());
		// Send the Kafka Message for successful Cancellation status
		StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_CANCELLED);
		try {
			producer.send(JobMessageFactory.getUpdateStatusMessage(instance.getJobId(), statusUpdate, SPACE));
		} catch (JsonProcessingException jsonException) {
			jsonException.printStackTrace();
			logger.log(String.format(
					"Error sending Cancelled Status from Job %s: %s. The Job was cancelled, but its status will not be updated in the Job Manager.",
					instance.getJobId(), jsonException.getMessage()), PiazzaLogger.ERROR);
		}
	}
}
