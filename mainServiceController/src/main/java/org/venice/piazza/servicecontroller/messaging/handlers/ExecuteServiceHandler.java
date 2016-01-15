package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license
// Remove System.out.println


import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.CoreServiceProperties;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreLogger;

import model.job.PiazzaJobType;
import model.job.metadata.ExecuteServiceData;
import model.job.metadata.ResourceMetadata;
import model.job.type.ExecuteServiceJob;
import model.job.type.RegisterServiceJob;



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
	
	private final static Logger LOGGER = Logger.getLogger(ExecuteServiceHandler.class);

	

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
		if (job != null)  {
			// Get the ResourceMetadata
			ExecuteServiceData esData = job.data;

			String result = handle(esData);
			if (result.length() > 0) {
				String jobId = job.getJobId();
				// TODO Use the result, send a message with the resource ID
				// and jobId
				
			}
			else {
				LOGGER.error("No result response from the handler, something went wrong");
			}
		}
		
	}//handle
	
	/**
	 * Handles requests to execute a service.  T
	 * TODO this needs to change to levarage pz-jbcommon ExecuteServiceMessage
	 * after it builds.
	 * @param message
	 * @return the Response as a String
	 */
	public String handle (ExecuteServiceData data) {
		ResponseEntity<String> responseString = null;
		// Get the id from the data
		String resourceId = data.getResourceId();
		ResourceMetadata rMetadata = accessor.getResourceById(resourceId);
		// Now get the mimeType for the request not using for now..
		String requestMimeType = rMetadata.requestMimeType;
		MultiValueMap<String, Object> map = new LinkedMultiValueMap<String, Object>();
		
		// Loop Through and add the parameters for the call
		Iterator  it = data.dataInputs.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	        map.add((String)pair.getKey(), pair.getValue());
	    }
	    // Determine the method type to execute the service
	    // Just handling Post and get for now
	    if (rMetadata.method.toUpperCase().equals("POST")) {

	    	responseString = template.postForEntity(rMetadata.url, map, String.class);
	    	LOGGER.info("The Response is " + responseString.toString());
	    
	    }
	    else if (rMetadata.method.toUpperCase().equals("GET")) {
	    	responseString = template.getForEntity(rMetadata.url, String.class, map);
	    	LOGGER.info("The Response is " + responseString.toString());
	    	

	    }
	 
	  	return responseString.toString();
		
	}

}