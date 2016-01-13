package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license



import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.venice.piazza.servicecontroller.data.model.ExecuteServiceMessage;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import model.job.PiazzaJobType;
//import model.job.metadata.ResourceMetadata;
import org.venice.piazza.servicecontroller.model.ResourceMetadata;

import model.job.type.ExecuteServiceJob;
import model.job.type.RegisterServiceJob;
/**
 * Handler which handles RegisterServiceJobs
 * @author mlynum
 *
 */
public class ExecuteServiceHandler implements PiazzaJobHandler {
	@Autowired
	private MongoAccessor accessor;

	public ExecuteServiceHandler(MongoAccessor accessor) {
		this.accessor = accessor;
	}

    /*
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public void handle (PiazzaJobType jobRequest ) {
		ExecuteServiceJob job = (ExecuteServiceJob)jobRequest;
		// Get the ResourceMetadata
		/*ResourceMetadata rMetadata = job.metadata;
		String result = handle(rMetadata); */
		/*if (result.length() > 0) {
			String jobId = job.getJobId();
			// TODO Use the result, send a message with the resource ID
			
		}*/
		
		
	}//handle
	
	public String handle (ExecuteServiceMessage message) {
		String result = "";
		// TODO Now call the UUID generator service
		// http://localhost:8080/uuid returns a uuid
		
		/*Random rand = new Random();		
		rMetadata.resourceId = "123-345-456" + rand.nextInt(50) + 2;
		
		String result = accessor.save(rMetadata);
		System.out.println("The result is " + result);
		// If an ID was returned then send a kafka message back updating the job iD 
		// with the resourceID */
		return result;
				
	}

}