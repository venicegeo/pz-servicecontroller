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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

/**
 * This Worker will make the direct REST request to that User Service, and update the Instance table based on the status
 * and poll time.
 * <p>
 * This component is responsible for polling the status of asynchronous user service instances. It will use the Mongo
 * AsyncServiceInstances collection in order to store persistence related to each running instance of an asynchronous
 * user service. This component will poll each instance, at a regular interval, and make a note of its status and query
 * time.
 * </p>
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class PollStatusWorker {
	@Autowired
	private MongoAccessor accessor;

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

	}
}
