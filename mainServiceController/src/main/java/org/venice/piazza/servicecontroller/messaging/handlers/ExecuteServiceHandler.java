package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license



import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.CoreServiceProperties;
import org.venice.piazza.servicecontroller.data.model.ExecuteServiceMessage;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import model.job.PiazzaJobType;

import model.job.type.ExecuteServiceJob;

/**
 * Handler which handles RegisterServiceJobs
 * @author mlynum
 *
 */

public class ExecuteServiceHandler implements PiazzaJobHandler {

	private MongoAccessor accessor;

	private CoreServiceProperties coreServiceProp;
	
	private RestTemplate template;
	

	public ExecuteServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProp) {
		this.accessor = accessor;
		this.coreServiceProp = coreServiceProp;
		this.template = new RestTemplate();
	
	}

    /*
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public void handle (PiazzaJobType jobRequest ) {
		ExecuteServiceJob job = (ExecuteServiceJob)jobRequest;
		
	}//handle
	
	public String handle (ExecuteServiceMessage message) {
		String result = "";
		// Get the ID from the message
		
		return result;
				
	}

}