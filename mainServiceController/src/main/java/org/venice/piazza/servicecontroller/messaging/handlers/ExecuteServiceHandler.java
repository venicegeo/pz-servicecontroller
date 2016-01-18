package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license
// Remove System.out.println


import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
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
	private CoreLogger coreLogger;
	private CoreServiceProperties coreServiceProperties;
	
	private RestTemplate template;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteServiceHandler.class);

	

	public ExecuteServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProperties, CoreLogger coreLogger) {
		this.accessor = accessor;
		this.coreServiceProperties = coreServiceProperties;
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
		
		LOGGER.debug("Executing a service");
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
		if (data.dataInputs.size() > 0) {
			// Loop Through and add the parameters for the call
			Iterator<Entry<String, String>>  it = data.dataInputs.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        LOGGER.debug(pair.getKey() + " = " + pair.getValue());
		        map.add((String)pair.getKey(), pair.getValue());
		    }
		    // Determine the method type to execute the service
		    // Just handling Post and get for now
		    if (rMetadata.method.toUpperCase().equals("POST")) {
	
		    	responseString = template.postForEntity(rMetadata.url, map, String.class);
		    	LOGGER.debug("The Response is " + responseString.toString());	
		 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.resourceId + " was executed with the following result " + responseString, CoreLogger.INFO);
		    
		    }
		    else if (rMetadata.method.toUpperCase().equals("GET")) {
		    	responseString = template.getForEntity(rMetadata.url, String.class, map);
		    	LOGGER.debug("The Response is " + responseString.toString());
		 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.resourceId + " was executed with the following result " + responseString, CoreLogger.INFO);
	
		    }
		}
		else {
			
			// Check to see if there is a value in the dataInput assume JSON mass post
			if (data.dataInput.length() > 0) {
				if (rMetadata.method.toUpperCase().equals("POST")) {
					HttpHeaders headers = new HttpHeaders();
					
					// Set the mimeType of the request
					MediaType mediaType = new MediaType(rMetadata.requestMimeType);
					headers.setContentType(mediaType);
					LOGGER.debug("Json to be used " + data.dataInput);
					LOGGER.debug("Mimetype is " + mediaType.getType());
					HttpEntity<String> requestEntity = new HttpEntity<String>(data.dataInput,headers);
					responseString = template.postForEntity(rMetadata.url, requestEntity, String.class);
			 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.resourceId + " was executed with the following result " + responseString, CoreLogger.INFO);

				}
				else if (rMetadata.method.toUpperCase().equals("GET")) {
					responseString = template.getForEntity(rMetadata.url, String.class, data.dataInput);
			 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.resourceId + " was executed with the following result " + responseString, CoreLogger.INFO);
			 	}
				
			}
			
		}
	 
	  	return responseString.toString();
		
	}

}