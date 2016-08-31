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
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.job.result.type.ErrorResult;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import util.PiazzaLogger;

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
		String url = String.format("%s/%s", service.getUrl(), STATUS_ENDPOINT);
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
			} else if (status.getStatus().equals(StatusUpdate.STATUS_SUCCESS)) {
				// TODO: Handle success
			} else if ((status.getStatus().equals(StatusUpdate.STATUS_ERROR)) || (status.getStatus().equals(StatusUpdate.STATUS_FAIL))
					|| (status.getStatus().equals(StatusUpdate.STATUS_CANCELLED))) {
				// Errors encountered. Report this and bubble it back up through the Job ID.
				processErrorStatus(instance, status);
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
		if (instance.getNumberErrorResponses() < STATUS_ERROR_LIMIT) {
			// Failure threshold has been reached. Fail the job.
			logger.log(String.format(
					"Job ID %s for Service ID %s Instance ID %s has failed too many times during periodic Status Checks. This Job is being marked as a failure.",
					instance.getJobId(), instance.getServiceId(), instance.getInstanceId()), PiazzaLogger.ERROR);
			// Remove this from the Collection of tracked instance Jobs.
			accessor.deleteAsyncServiceInstance(instance.getJobId());
			// Send a Failure message back to the Job Manager via Kafka.
			// TODO: 
		} else {
			// Update the Database that this instance has failed.
			accessor.updateAsyncServiceInstance(instance);
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
	private void processErrorStatus(AsyncServiceInstance instance, StatusUpdate serviceStatus) {
		// Remove the Instance from the Instance Table
		accessor.deleteAsyncServiceInstance(instance.getJobId());

		// Form the message.
		String error = String.format("Instance %s reported back Status %s. ", instance.getInstanceId(), serviceStatus.getStatus());
		if (serviceStatus.getResult() instanceof ErrorResult) {
			// If we can parse any further details on the error, then do so here.
			ErrorResult errorResult = (ErrorResult) serviceStatus.getResult();
			error = String.format("%s Details: %s, %s", error, errorResult.getMessage(), errorResult.getDetails());
		}

		// Create a new Status Update to send to the Job Manager.
		StatusUpdate statusUpdate = new StatusUpdate();
		statusUpdate.setStatus(serviceStatus.getStatus());
		// Create the Message for the Error Result of the Status
		ErrorResult errorResult = new ErrorResult();
		errorResult.setMessage(error);
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
	 * Determines if the Status is done processing or not.
	 * 
	 * @param status
	 *            The Status.
	 * @return True if done, false if not.
	 */
	private boolean isDoneProcessing(String status) {
		if ((status.equals(StatusUpdate.STATUS_ERROR)) || (status.equals(StatusUpdate.STATUS_FAIL))
				|| (status.equals(StatusUpdate.STATUS_SUCCESS))) {
			return true;
		} else {
			return false;
		}
	}
}
