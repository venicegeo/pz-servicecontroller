package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license


import java.util.ArrayList;
import java.util.List;

import model.job.PiazzaJobType;
import model.job.type.DeleteServiceJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import org.venice.piazza.servicecontroller.util.CoreUUIDGen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Handler for handling registerService requests.  This handler is used 
 * when register-service kafka topics are received or when clients utilize the 
 * ServiceController registerService web service.
 * @author mlynum
 * @version 1.0
 *
 */

public class DeleteServiceHandler implements PiazzaJobHandler {
	private MongoAccessor accessor;
	private CoreLogger coreLogger;
	private CoreUUIDGen coreUuidGen;
	private static final Logger LOGGER = LoggerFactory.getLogger(DeleteServiceHandler.class);


	public DeleteServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProp, CoreLogger coreLogger, CoreUUIDGen coreUuidGen){ 
		this.accessor = accessor;
		this.coreLogger = coreLogger;
		this.coreUuidGen = coreUuidGen;
	
	}

    /*
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest ) {
		
		LOGGER.debug("Updating a service");
		DeleteServiceJob job = (DeleteServiceJob)jobRequest;
		if (job != null)  {
			// Get the ResourceMetadata
			String resourceId = job.serviceID;

			String result = handle(resourceId);
			if (result.length() > 0) {
				String jobId = job.getJobId();
				// TODO Use the result, send a message with the resource ID
				// and jobId
				ArrayList<String> resultList = new ArrayList<String>();
				resultList.add(jobId);
				resultList.add(resourceId);
				ResponseEntity<List<String>> handleResult = new ResponseEntity<List<String>>(resultList,HttpStatus.OK);
				return handleResult;
				
			}
			else {
				LOGGER.error("No result response from the handler, something went wrong");
				ArrayList<String> errorList = new ArrayList<String>();
				errorList.add("DeleteServiceHandler handle didn't work");
				ResponseEntity<List<String>> errorResult = new ResponseEntity<List<String>>(errorList,HttpStatus.METHOD_FAILURE);
				
				return errorResult;
			}
		}
		else {
			return null;
		}
	}//handle
	
	/**
	 * 
	 * @param rMetadata
	 * @return resourceID of the registered service
	 */
	public String handle (String resourceId) {

        coreLogger.log("about to delete a registered service.", CoreLogger.INFO);
     
        ObjectMapper mapper = new ObjectMapper();
		
		String result= "";
		try {
			result = mapper.writeValueAsString(accessor.delete(resourceId));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOGGER.debug("The result of the delete is " + result);
		if (result.length() > 0) {
		   coreLogger.log("The service with id " + resourceId + " was deleted " + result, CoreLogger.INFO);
		} else {
			   coreLogger.log("The service with id " + resourceId + " was NOT deleted", CoreLogger.INFO);
		}
		// If an ID was returned then send a kafka message back updating the job iD 
		// with the resourceID
		return result;
				
	}
	


}
