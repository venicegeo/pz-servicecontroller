package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license



import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.CoreServiceProperties;
import org.venice.piazza.servicecontroller.data.model.ExecuteServiceMessage;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import model.job.PiazzaJobType;
import model.job.metadata.ExecuteServiceData;
import model.job.type.ExecuteServiceJob;



/**
 * Handler for handling executeService requests.  This handler is used 
 * when execute-service kafka topics are received or when clients utilize the 
 * ServiceController service.
 * @author mlynum
 * @version 1.0
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
     * Handler for handling execute service requets.  This
     * method will execute a service given the resourceId and return a response to
     * the job manager.
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public void handle (PiazzaJobType jobRequest ) {
		ExecuteServiceJob job = (ExecuteServiceJob)jobRequest;
		
	}//handle
	
	/**
	 * Handles requests to execute a service.  T
	 * TODO this needs to change to levarage pz-jbcommon ExecuteServiceMessage
	 * after it builds.
	 * @param message
	 * @return
	 */
	public String handle (ExecuteServiceData data) {
		String result = "";
		// Get the ID from the message
		
		return result;
				
	}

}