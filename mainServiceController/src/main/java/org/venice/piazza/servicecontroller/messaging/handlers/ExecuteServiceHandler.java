package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license
// Remove System.out.println



import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import model.job.PiazzaJobType;
import model.job.metadata.ExecuteServiceData;
import model.job.metadata.ResourceMetadata;
import model.job.type.ExecuteServiceJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;



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
		this.coreLogger = coreLogger;
	
	}

    /*
     * Handler for handling execute service requets.  This
     * method will execute a service given the resourceId and return a response to
     * the job manager.
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest ) {
		ExecuteServiceJob job = (ExecuteServiceJob)jobRequest;
		
		LOGGER.debug("Executing a service");
		if (job != null)  {
			// Get the ResourceMetadata
			ExecuteServiceData esData = job.data;

			ResponseEntity<String> handleResult = handle(esData);
			ArrayList<String> resultList = new ArrayList<String>();
			resultList.add(handleResult.getBody());
			ResponseEntity<List<String>> result = new ResponseEntity<List<String>>(resultList,handleResult.getStatusCode());
			

			// TODO Use the result, send a message with the resource ID
			// and jobId
			return result;
				
		
		}
		else {
			return null;
		}
		
	}//handle
	
	/**
	 * Handles requests to execute a service.  T
	 * TODO this needs to change to levarage pz-jbcommon ExecuteServiceMessage
	 * after it builds.
	 * @param message
	 * @return the Response as a String
	 */
	public ResponseEntity<String> handle (ExecuteServiceData data) {
		ResponseEntity<String> responseEntity = null;
		// Get the id from the data
		String resourceId = data.getResourceId();
		ResourceMetadata rMetadata = accessor.getResourceById(resourceId);
		// Now get the mimeType for the request not using for now..
		String requestMimeType = rMetadata.requestMimeType;
		MultiValueMap<String, String> map = new LinkedMultiValueMap<String, String>();
	
        // If there are parameters
		if (data.dataInputs.size() > 0) {
			LOGGER.info("number of inputs are" + data.dataInputs.size());
			LOGGER.info("The inputs are " + data.dataInputs.toString());
			
			UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(rMetadata.url);
			
			// Loop Through and add the parameters for the call
			Iterator<Entry<String, String>>  it = data.dataInputs.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        LOGGER.debug(pair.getKey() + " = " + pair.getValue());
		        map.add((String)pair.getKey(), (String)pair.getValue());

		        builder.queryParam((String)pair.getKey(), (String)pair.getValue());
		    }
		    
		    
		    // Determine the method type to execute the service
		    // Just handling Post and get for now

		    // If there is no Json or body coming with this the just execute
		    if (data.dataInput != null && data.dataInput.length() <= 0) {
			    if (rMetadata.method.toUpperCase().equals("POST")) {
			    	LOGGER.debug("The url to be executed is " + rMetadata.url);
			    	responseEntity = template.postForEntity(rMetadata.url, map, String.class);
			    	LOGGER.debug("The Response is " + responseEntity.toString());	
			 		coreLogger.log("Service with resourceID " + resourceId + " was executed with the following result " + responseEntity, CoreLogger.INFO);
			    
			    }
			    else if (rMetadata.method.toUpperCase().equals("GET")) {
			    	LOGGER.debug("The map of parameters is " + map.size());
			    	LOGGER.debug("The url to be executed is " + rMetadata.url);
			    	LOGGER.debug("The built URI is  " + builder.toUriString());
			    	responseEntity = template.getForEntity(builder.toUriString(), String.class, map);
			    	LOGGER.debug("The Response is " + responseEntity.toString());
			 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.id + " was executed with the following result " + responseEntity, CoreLogger.INFO);
		
			    }
			} 
		    // There are parameters and a request BODY (POST) so handle 
		    else {
				// If it is a post with a body then build the enttiy adding teh body
				if (rMetadata.method.toUpperCase().equals("POST")) {
					HttpEntity<String> requestEntity = buildHttpEntity(rMetadata, map, data.dataInput);
					LOGGER.debug("The url to be executed is " + rMetadata.url);
			    	responseEntity = template.postForEntity(rMetadata.url, requestEntity, String.class);
			    	LOGGER.debug("The Response is " + responseEntity.toString());	
			 		coreLogger.log("Service with resourceID " + resourceId + " was executed with the following result " + responseEntity, CoreLogger.INFO);

				}
			}
		}// If there were named parameters
		// Else there were no named parameters so if there is just a blob of data

		else {
			
			// Check to see if there is a value in the dataInput assume JSON mass post
			if (data.dataInput.length() > 0) {
				if (rMetadata.method.toUpperCase().equals("POST")) {
					
					HttpHeaders headers = new HttpHeaders();
					
					// Set the mimeType of the request
					MediaType mediaType = createMediaType(rMetadata.requestMimeType);
					headers.setContentType(mediaType);
					LOGGER.debug("Json to be used " + data.dataInput);
					LOGGER.debug("Mimetype is " + mediaType.getType());
					HttpEntity<String> requestEntity = new HttpEntity<String>(data.dataInput,headers);

				
					responseEntity = template.postForEntity(rMetadata.url, requestEntity, String.class);
					LOGGER.debug("Service " + rMetadata.name + " with resourceID " + rMetadata.id + " was executed with the following result " + responseEntity.getBody());
			 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.id + " was executed with the following result " + responseEntity.getBody(), CoreLogger.INFO);


				}
				else if (rMetadata.method.toUpperCase().equals("GET")) {
					LOGGER.debug("Json to be used " + data.dataInput);

					String uri = rMetadata.url + "?" + data.dataInput;
					responseEntity = template.getForEntity(uri, String.class);
			 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.id + " was executed with the following result " + responseEntity.getBody(), CoreLogger.INFO);

			 	}
				
			}
			// There are no parameters, just make a call
			else {
				
				if (rMetadata.method.toUpperCase().equals("POST")) {
					HttpHeaders headers = new HttpHeaders();
					MediaType mediaType = createMediaType(rMetadata.requestMimeType);
					headers.setContentType(mediaType);
					LOGGER.debug("Calling URL POST " + rMetadata.url);
					LOGGER.debug("Mimetype is " + mediaType.getType() + mediaType.getSubtype());

					responseEntity = template.postForEntity(rMetadata.url, null, String.class);
					LOGGER.debug("Service " + rMetadata.name + " with resourceID " + rMetadata.id + " was executed with the following result " + responseEntity.getBody());
			 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.id + " was executed with the following result " + responseEntity.getBody(), CoreLogger.INFO);


				}
				else if (rMetadata.method.toUpperCase().equals("GET")) {
					LOGGER.debug("Calling URL GET" + rMetadata.url);
					responseEntity = template.getForEntity(rMetadata.url, String.class);
			 		coreLogger.log("Service " + rMetadata.name + " with resourceID " + rMetadata.id + " was executed with the following result " + responseEntity.getBody(), CoreLogger.INFO);

			 	}
				
			}
			
		}
	 
	  	return responseEntity;
		
	}
	
	/**
	 * This method creates a MediaType based on the mimetype that was 
	 * provided
	 * @param mimeType
	 * @return MediaType
	 */
	private MediaType createMediaType(String mimeType) {
		MediaType mediaType;
		String type, subtype;
		StringBuffer sb = new StringBuffer(mimeType);
		int index = sb.indexOf("/");
		// If a slash was found then there is a type and subtype
		if (index != -1) {
			type = sb.substring(0, index);
			
		    subtype = sb.substring(index+1, mimeType.length());
		    mediaType = new MediaType(type, subtype);
		    LOGGER.debug("The type is="+type);
			LOGGER.debug("The subtype="+subtype);
		}
		else {
			// Assume there is just a type for the mime, no subtype
			mediaType = new MediaType(mimeType);			
		}
		
		return mediaType;
	
		
	}
	
	public HttpEntity<String> buildHttpEntity(ResourceMetadata rMetadata, MultiValueMap<String, String> headers, String data) {
	
		
		// Set the mimeType of the request
		headers.add("Content-type", rMetadata.requestMimeType);
		//MediaType mediaType = createMediaType(rMetadata.requestMimeType);
		//headers.setContentType(mediaType);
		LOGGER.debug("data to be used " + data);
		LOGGER.debug("Mimetype is " + rMetadata.requestMimeType);
		HttpEntity<String> requestEntity = new HttpEntity<String>(data,headers);
		return requestEntity;
	
	}

}