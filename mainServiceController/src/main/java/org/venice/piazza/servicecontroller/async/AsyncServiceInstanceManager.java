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
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

/**
 * This component manages the full cycle of Asynchronous User Service Instances. It handles execution, polling, and
 * processing results.
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class AsyncServiceInstanceManager {
	/**
	 * The number of seconds that the Service Controller will query an Asynchronous User Service for status.
	 */
	public static final int STALE_INSTANCE_THRESHOLD_SECONDS = 10;
	/**
	 * The frequency (in seconds) that Asynchronous User Services will be polled.
	 */
	public static final int POLL_FREQUENCY_SECONDS = 10;

	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private PollStatusWorker pollStatusWorker;

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
				pollStatusWorker.pollStatus(instance);
			}
		}
	}
}
