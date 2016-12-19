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
package org.venice.piazza.servicecontroller.controller;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import model.response.PiazzaResponse;
import model.status.StatusUpdate;
import util.PiazzaLogger;

/**
 * REST Controller for Task-Managed Service endpoints. This includes pulling Jobs off the queue, and updating Status for
 * jobs. Also metrics such as queue length are available.
 * 
 * @author Patrick.Doody
 *
 */
@RestController
public class TaskManagedController {
	@Autowired
	private PiazzaLogger piazzaLogger;

	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceController.class);

	/**
	 * Pulls the next job off of the Service Queue.
	 * 
	 * @param username
	 *            The name of the user. Used for verification.
	 * @param serviceId
	 *            The ID of the Service
	 * @return The information for the next Job, if one is present.
	 */
	public ResponseEntity<PiazzaResponse> getNextServiceJobFromQueue(String username, String serviceId) {
		return null;
	}

	/**
	 * Updates the Status for a Piazza Job.
	 * 
	 * @param username
	 *            The name of the user. Used for verification.
	 * @param jobId
	 *            The ID of the Job to update
	 * @param statusUpdate
	 *            The update contents, including status, percentage, and possibly results.
	 * @return Success or error.
	 */
	public ResponseEntity<PiazzaResponse> updateServiceJobStatus(String username, String jobId, StatusUpdate statusUpdate) {
		return null;
	}

	/**
	 * Gets metadata for a specific Task-Managed Service.
	 * 
	 * @param username
	 *            The name of the user. Used for verification.
	 * @param serviceId
	 *            The ID of the Service
	 * @return Map containing information regarding the Task-Managed Service
	 */
	public Map<String, String> getServiceQueueData(String username, String serviceId) {
		return null;
	}
}
