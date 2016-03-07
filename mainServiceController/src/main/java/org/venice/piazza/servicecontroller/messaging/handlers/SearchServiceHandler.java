package org.venice.piazza.servicecontroller.messaging.handlers;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.job.PiazzaJobType;
import model.job.metadata.Service;
import model.job.type.SearchServiceJob;
import model.service.SearchCriteria;
import util.PiazzaLogger;


/**
 * Handler for handling search requests.  Searches the databse for 
 * registered services using the name and/or ID provide
 * @author mlynum
 * @version 1.0
 *
 */


public class SearchServiceHandler implements PiazzaJobHandler {
	
	private MongoAccessor accessor;
	private PiazzaLogger coreLogger;
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchServiceHandler.class);


	public SearchServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProp, PiazzaLogger coreLogger){ 
		this.accessor = accessor;
		this.coreLogger = coreLogger;

	
	}
	/*
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest ) {
		
		LOGGER.debug("Search a service");
		SearchServiceJob job = (SearchServiceJob)jobRequest;

		ArrayList<String> retVal = new ArrayList<String>();
		// Get the criteria to use for the search
		SearchCriteria criteria = job.data;
		
        ResponseEntity<String> response = handle(criteria);

		retVal.add(response.getBody());
		        
		return new ResponseEntity<List<String>>(retVal,response.getStatusCode());

	}//handle
	
	/**
	 * 
	 * @param criteria to search.  field and regex expression
	 * @return a String of ResourceMetadata items that match the search
	 */
	public ResponseEntity<String> handle (SearchCriteria criteria) {
		ResponseEntity<String> responseEntity = null;
        String result = null;
        coreLogger.log("About to search using criteria " + criteria, PiazzaLogger.INFO);

		List <Service> results = accessor.search(criteria);
		if (results.size() <= 0) {
		   coreLogger.log("No results were returned searching for field " + 
				   		criteria.getField() + 
				   		" and search criteria " + 
				   		criteria.getPattern(), PiazzaLogger.INFO);
			responseEntity = new ResponseEntity<String>(result, HttpStatus.NO_CONTENT);

		}
		else {
			ObjectMapper mapper = new ObjectMapper();
			try {
			result = mapper.writeValueAsString(results);
			responseEntity = new ResponseEntity<String>(result, HttpStatus.OK);
			} catch (JsonProcessingException jpe) {
				coreLogger.log("There was a problem generating the Json response", PiazzaLogger.ERROR);
			}
		}

		return responseEntity;
				
	}
}
