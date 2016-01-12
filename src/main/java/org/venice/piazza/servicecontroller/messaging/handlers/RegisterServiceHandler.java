package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license

import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.type.RegisterServiceJob;
/**
 * Handler which handles RegisterServiceJobs
 * @author mlynum
 *
 */
public class RegisterServiceHandler implements Handler {
	private MongoAccessor accessor;

	public RegisterServiceHandler(MongoAccessor accessor) {
		this.accessor = accessor;
	}

    /*
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public void handle (PiazzaJobType rsJob ) {
		RegisterServiceJob job = (RegisterServiceJob)rsJob;
		// Get the ResourceMetadata
		ResourceMetadata rMetadata = job.metadata;
		// TODO Now call the UUID generator service
		// http://localhost:8080/uuid returns a uuid
		rMetadata.serviceID = "123-345-456";
		
		String result = accessor.save(rMetadata);
		// If an ID was returned then send a kafka message back updating the job iD 
		// with the resourceID
		if (result.length() > 0) {
			String jobId = job.getJobId();
			// TODO Use the result, send a message with the resource ID
			
		}
		
	}

}
