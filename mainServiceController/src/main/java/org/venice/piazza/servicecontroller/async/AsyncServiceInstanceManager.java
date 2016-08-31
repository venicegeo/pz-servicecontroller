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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import util.PiazzaLogger;

/**
 * This component manages the full cycle of Asynchronous User Service Instances. It handles execution, polling, and
 * processing results.
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class AsyncServiceInstanceManager {
	@Value("${async.stale.instance.threshold.seconds}")
	private int STALE_INSTANCE_THRESHOLD_SECONDS;
	@Value("${async.poll.frequency.seconds}")
	private int POLL_FREQUENCY_SECONDS;
	@Value("${async.status.error.limit}")
	private int STATUS_ERROR_LIMIT;

	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private PollStatusWorker pollStatusWorker;
	@Autowired
	private PiazzaLogger logger;

	private PollServiceTask pollTask = new PollServiceTask();
	private Timer pollTimer = new Timer();

	/**
	 * Begins scheduled polling of asynchronous job status.
	 */
	@PostConstruct
	public void startPolling() {
		// Begin polling at the determined frequency
		pollTimer.schedule(pollTask, 0, POLL_FREQUENCY_SECONDS * 1000);
	}

	/**
	 * Halts scheduled polling of asynchronous job status.
	 */
	public void stopPolling() {
		pollTimer.cancel();
	}

	/**
	 * Timer Task that will, on a schedule, poll for the Status of Stale asynchronous user services.
	 * <p>
	 * This component is responsible for polling the status of asynchronous user service instances. It will use the
	 * Mongo AsyncServiceInstances collection in order to store persistence related to each running instance of an
	 * asynchronous user service. This component will poll each instance, at a regular interval, and make a note of its
	 * status and query time.
	 * </p>
	 */
	public class PollServiceTask extends TimerTask {
		/**
		 * Polls all stale Asynchronous Service Instances (instances that haven't been checked since the last threshold
		 * time)
		 */
		@Override
		public void run() {
			// Get the list of all stale User Services and poll each.
			List<AsyncServiceInstance> staleInstances = accessor.getStaleServiceInstances();
			for (AsyncServiceInstance instance : staleInstances) {
				// Ensure the Service has not failed an inordiante number of times.
				if (instance.getNumberErrorResponses() < STATUS_ERROR_LIMIT) {
					// Poll for Status
					pollStatusWorker.pollStatus(instance);
				} else {
					// The Server has timed out too often. Send a failure Status.
					logger.log(String.format(
							"Job ID %s for Service ID %s Instance ID %s has failed too many times during periodic Status Checks. This Job is being marked as a failure.",
							instance.getJobId(), instance.getServiceId(), instance.getInstanceId()), PiazzaLogger.ERROR);
					// Remove this from the Collection of tracked instance Jobs.
					accessor.deleteAsyncServiceInstance(instance.getJobId());
					// TODO: Mark as failure
				}
			}
		}
	}
}
