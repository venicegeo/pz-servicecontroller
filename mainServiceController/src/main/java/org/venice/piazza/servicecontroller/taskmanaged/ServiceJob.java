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

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a Job that has been sent to a Task-Managed Piazza Service.
 * <p>
 * This object correlates the Job ID with the time that is was pulled off the Jobs queue and sent to an external Service
 * Worker for processing.
 * </p>
 * 
 * @author Patrick.Doody
 *
 */
public class ServiceJob {
	public String jobId;
	/**
	 * The time that work began processing on this Job.
	 */
	@JsonIgnore
	public DateTime startedOn;
	/**
	 * The time that the Job was inserted into the Jobs queue.
	 */
	@JsonIgnore
	public DateTime queuedOn;

	public ServiceJob() {
		this.queuedOn = new DateTime();
	}

	public ServiceJob(String jobId) {
		this();
		this.jobId = jobId;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	@JsonIgnore
	public DateTime getStartedOn() {
		return startedOn;
	}

	@JsonIgnore
	public void setStartedOn(DateTime startedOn) {
		this.startedOn = startedOn;
	}

	@JsonProperty("startedOn")
	public String getStartedOnString() {
		if (startedOn != null) {
			// Defaults to ISO8601
			return startedOn.toString();
		} else {
			return null;
		}
	}

	@JsonProperty("startedOn")
	public void setStartedOnString(String startedOn) {
		this.startedOn = new DateTime(startedOn);
	}

	@JsonIgnore
	public DateTime getQueuedOn() {
		return queuedOn;
	}

	@JsonIgnore
	public void setQueuedOn(DateTime queuedOn) {
		this.queuedOn = queuedOn;
	}

	@JsonProperty("queuedOn")
	public String getQueuedOnString() {
		if (queuedOn != null) {
			// Defaults to ISO8601
			return queuedOn.toString();
		} else {
			return null;
		}
	}

	@JsonProperty("queuedOn")
	public void setQueuedOnString(String queuedOn) {
		this.queuedOn = new DateTime(queuedOn);
	}
}
