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

import java.util.ArrayList;
import java.util.List;

/**
 * Model for managing a Task Management Queue for a Service.
 * 
 * @author Patrick.Doody
 *
 */
public class ServiceQueue {
	private String serviceId;
	private List<ServiceJob> jobs = new ArrayList<ServiceJob>();

	public ServiceQueue() {

	}

	public ServiceQueue(String serviceId) {
		this.serviceId = serviceId;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	/**
	 * Returns the list of Jobs (referenced by Job ID) that are currently in this Service's queue.
	 * 
	 * @return List of Job IDs currently pending, or in progress.
	 */
	public List<ServiceJob> getJobs() {
		return jobs;
	}

	public void setJobs(List<ServiceJob> jobs) {
		this.jobs = jobs;
	}
}
