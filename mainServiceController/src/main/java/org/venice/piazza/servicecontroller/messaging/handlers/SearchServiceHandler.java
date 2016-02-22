package org.venice.piazza.servicecontroller.messaging.handlers;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import org.venice.piazza.servicecontroller.util.CoreUUIDGen;

import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.type.RegisterServiceJob;
import util.PiazzaLogger;

/**
 * Handler for handling search requests.  Searches the databse for 
 * registered services using the name and/or ID provide
 * @author mlynum
 * @version 1.0
 *
 */


public class SearchServiceHandler { //implements PiazzaJobHandler {
	
	private MongoAccessor accessor;
	private PiazzaLogger coreLogger;
	private CoreUUIDGen coreUuidGen;
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchServiceHandler.class);


	public SearchServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProp, PiazzaLogger coreLogger, CoreUUIDGen coreUuidGen){ 
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
//	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest ) {
//		
//		LOGGER.debug("Search a service");
		//SearchServiceJob job = (SearchServiceJob)jobRequest;
//		if (job != null)  {
//			// Get the ResourceMetadata
//		//	model.job.metadata.SearchQuery queryparams = job.data;
//
//			String result = handle(rMetadata);
//			if (result.length() > 0) {
				//String jobId = job.getJobId();
				// TODO Use the result, send a message with the resource ID
				// and jobId
				//ArrayList<String> resultList = new ArrayList<String>();
				//resultList.add(jobId);
				//resultList.add(rMetadata.id);
				
				
				//ResponseEntity<List<String>> handleResult = new ResponseEntity<List<String>>(resultList,HttpStatus.OK);
				
//				String responseString = "{\"resourceId\":" + "\"" + result + "\"}";
//				ArrayList<String> resultList = new ArrayList<String>();
//				resultList.add(responseString);
//				ResponseEntity<List<String>> handleResult = new ResponseEntity<List<String>>(resultList,HttpStatus.OK);
//
//				return handleResult;
//				
//			}
//			else {
//				LOGGER.error("No result response from the handler, something went wrong");
//				ArrayList<String> errorList = new ArrayList<String>();
//				errorList.add("RegisterServiceHandler handle didn't work");
//				ResponseEntity<List<String>> errorResult = new ResponseEntity<List<String>>(errorList,HttpStatus.METHOD_FAILURE);
//				
//				return errorResult;
//			}
//		}
//		else {
//			return null;
//		}
//	}//handle
	
//	**
//	 * 
//	 * @param queryParams
//	 * @return List of ResourceMetadata that matches the search
//	 */
//	public String handle (SearchQuery query) {
//
//       //coreLogger.log("about to save a registered service.", CoreLogger.INFO);
//
//		rMetadata.id = coreUuidGen.getUUID();
//		String result = accessor.save(rMetadata);
//		LOGGER.debug("The result of the save is " + result);
//		if (result.length() > 0) {
//		   //coreLogger.log("The service " + rMetadata.name + " was stored with id " + result, CoreLogger.INFO);
//		} else {
//		//	   coreLogger.log("The service " + rMetadata.name + " was NOT stored", CoreLogger.INFO);
//		}
//		// If an ID was returned then send a kafka message back updating the job iD 
//		// with the resourceID
//		return result;
//				
//	}
}
