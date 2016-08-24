package org.venice.piazza.servicecontroller.messaging.handlers;

import java.util.List;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.job.Job;
import model.job.type.SearchServiceJob;
import model.service.SearchCriteria;
import model.service.metadata.Service;

import util.PiazzaLogger;


/**
 * Handler for handling search requests.  Searches the databse for 
 * registered services using the name and/or Id provide
 * @author mlynum
 * @version 1.0
 *
 */
@Component
public class SearchServiceHandler implements PiazzaJobHandler {
	
	@Autowired
	private MongoAccessor accessor;

	@Autowired
	private PiazzaLogger coreLogger;


	/**
	 * Handler for the RegisterServiceJob that was submitted. Stores the metadata in MongoDB (non-Javadoc)
	 * 
	 * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
	 */
	@Override
	public ResponseEntity<String> handle(Job jobRequest) {
		SearchServiceJob job = (SearchServiceJob) jobRequest.jobType;
		ResponseEntity<String>responseEntity;
		if ((job != null) && (job.data != null)) {
			// Get the criteria to use for the search
			SearchCriteria criteria = job.data;

			coreLogger.log("search " + " " + criteria.field + "->" + criteria.pattern, PiazzaLogger.INFO);
	
			ResponseEntity<String> response = handle(criteria);
			responseEntity = new ResponseEntity<>(response.getBody(), response.getStatusCode());
		}
		else {
			responseEntity = new ResponseEntity<>("Null request received.", HttpStatus.BAD_REQUEST);
		}
		return responseEntity;
	}

	/**
	 * 
	 * @param criteria
	 *            to search. field and regex expression
	 * @return a String of ResourceMetadata items that match the search
	 */
	public ResponseEntity<String> handle(SearchCriteria criteria) {
		ResponseEntity<String> responseEntity;
		String result;
		if (criteria != null) {
			coreLogger.log("About to search using criteria" + criteria, PiazzaLogger.INFO);
	
			List<Service> results = accessor.search(criteria);
			if (results.isEmpty()) {
				coreLogger.log(
						"No results were returned searching for field " + criteria.getField() + " and search criteria " + criteria.getPattern(),
						PiazzaLogger.INFO);
			
				responseEntity = new ResponseEntity<>("No results were returned searching for field", HttpStatus.NO_CONTENT);
			} else {
				ObjectMapper mapper = makeObjectMapper();
				try {
					result = mapper.writeValueAsString(results);
					responseEntity = new ResponseEntity<>(result, HttpStatus.OK);
				} catch (JsonProcessingException jpe) {
					// This should never happen, but still have to catch it
					coreLogger.log("There was a problem generating the Json response", PiazzaLogger.ERROR);
					responseEntity = new ResponseEntity<>("Could not search for services" , HttpStatus.NOT_FOUND);

				}
			}
		}
		else
			responseEntity = new ResponseEntity<>("No criteria was specified", HttpStatus.NO_CONTENT);

		return responseEntity;
	}
	
	ObjectMapper makeObjectMapper() {
		return new ObjectMapper();
	}
}
