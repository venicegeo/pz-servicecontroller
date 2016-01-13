package org.venice.piazza.servicecontroller.data.model;


import model.job.Job;
import model.request.PiazzaJobRequest;

import org.joda.time.DateTime;

/**
 * Wraps PiazzaJobRequests and PiazzaJobTypes into Job objects, which contain
 * metadata about that Job. The Job objects are ultimately what are stored in
 * the Job Manager's database.
 * 
 * From pz-jobmanager TODO need to discuss about moving to pz-jobcommon
 * 
 */
public class JobFactory {
	public static Job fromJobRequest(PiazzaJobRequest jobRequest, String jobId) {
		Job job = new Job();
		job.setJobId(jobId);
		job.jobType = jobRequest.jobType;
		job.submitterApiKey = jobRequest.apiKey;
		job.status = "Submitted"; // TODO: Enum
		job.submitted = new DateTime();
		return job;
	}
}