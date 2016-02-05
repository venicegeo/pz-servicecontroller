package org.venice.piazza.servicecontroller.messaging.handlers;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author mlynum
 * @version 1.0
 */
import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;

public class DescribeServiceHandler implements PiazzaJobHandler { 
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DescribeServiceHandler.class);
	
	private MongoAccessor accessor;
	private CoreLogger coreLogger;
	private CoreServiceProperties coreServiceProperties;	
	private RestTemplate template;
	
	public DescribeServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProperties, CoreLogger coreLogger) {
		this.accessor = accessor;
		this.coreServiceProperties = coreServiceProperties;
		this.template = new RestTemplate();
		this.coreLogger = coreLogger;
	
	}
	//TODO needs to be implemented
	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest ) {
		
		
		return null;
		
	}
	
	public ResponseEntity<String> handle (String resourceId) {
		ResponseEntity<String> responseEntity = null;
		
		try {
	
			ResourceMetadata rMetadata = accessor.getResourceById(resourceId);
			ObjectMapper mapper = new ObjectMapper();
			String result = mapper.writeValueAsString(rMetadata);
			responseEntity = new ResponseEntity<String>(result, HttpStatus.OK);
		} catch (Exception ex) {
			
			LOGGER.error(ex.getMessage());
			responseEntity = new ResponseEntity<String>("Could not retrieve resourceId " + resourceId, HttpStatus.NOT_FOUND);
			
		}
	
		return responseEntity;
		
	}

}
