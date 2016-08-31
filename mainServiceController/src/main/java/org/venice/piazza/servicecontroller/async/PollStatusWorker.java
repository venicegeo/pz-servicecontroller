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

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

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
	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private PiazzaLogger logger;

	private RestTemplate restTemplate = new RestTemplate();

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

			// Check if the service is done or not.
			if (isDoneProcessing(status.getStatus())) {
				// If this service is done, then process that.
				// TODO: Handle completed status
			} else {
				// If this service is not done, then mark the status and we'll poll again later.
				instance.setStatus(status);
				instance.setLastCheckedOn(new DateTime());
				accessor.updateAsyncServiceInstance(instance);
			}
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			// Increment the failure count
			instance.setNumberErrorResponses(instance.getNumberErrorResponses() + 1);
			// Update the Database that this instance has failed.
			logger.log(String.format(
					"HTTP Error Status %s encountered for Running Service ID %s Instance %s. The number of Errors has been increments (%s)",
					exception.getStatusCode().toString(), instance.getServiceId(), instance.getInstanceId(),
					instance.getNumberErrorResponses()), PiazzaLogger.WARNING);
			accessor.updateAsyncServiceInstance(instance);
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
		if ((status.equals(StatusUpdate.STATUS_CANCELLED)) || (status.equals(StatusUpdate.STATUS_ERROR))
				|| (status.equals(StatusUpdate.STATUS_FAIL)) || (status.equals(StatusUpdate.STATUS_SUCCESS))) {
			return true;
		} else {
			return false;
		}
	}
}
