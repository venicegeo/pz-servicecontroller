package org.venice.piazza.servicecontroller.controller;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.venice.piazza.servicecontroller.CoreServiceProperties;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreLogger;

import model.job.metadata.ResourceMetadata;
import model.job.metadata.ExecuteServiceData;

// TODO Add License

/** 
 * Purpose of this controller is to handle service requests
 * @author mlynum
 * @since 1.0
 */

@RestController
@RequestMapping("/servicecontroller")
public class ServiceController {
	private RegisterServiceHandler rsHandler;
	private ExecuteServiceHandler esHandler;
	@Autowired
	private MongoAccessor accessor;
	
	@Autowired
	private CoreServiceProperties coreServiceProp;
	
	@Autowired
	private CoreLogger coreLogger;
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceController.class);
	
	public ServiceController() {
		
	}
	/**
	 *  Initialize the handler to handle calls
	 */
	@PostConstruct
	public void initialize() {
		
		// Initialize calling server
		LOGGER.info("initialize: coreServiceDiscovery = " + coreServiceProp.getDiscoveryservice());
		rsHandler = new RegisterServiceHandler(accessor, coreServiceProp, coreLogger);
		esHandler = new ExecuteServiceHandler(accessor, coreServiceProp, coreLogger);
		
	
	}
	@RequestMapping(value = "/registerService", method = RequestMethod.POST, headers="Accept=application/json", produces=MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String registerService(@RequestBody ResourceMetadata serviceMetadata) {

		LOGGER.debug("serviceMetadata received is " + serviceMetadata);
	    String result = rsHandler.handle(serviceMetadata);
	    
	    LOGGER.debug("ServiceController: Result is" + "{\"resourceId:" + "\"" + result + "\"}");
	    String responseString = "{\"resourceId\":" + "\"" + result + "\"}";
	    
		return responseString;

	}
	
	@RequestMapping(value = "/executeService", method = RequestMethod.POST, headers="Accept=application/json")
	public @ResponseBody String executeService(@RequestBody ExecuteServiceData data) {
		LOGGER.debug("executeService resourceId=" + data.resourceId);
		LOGGER.debug("executeService datainput=" + data.dataInput);

		for (Map.Entry<String,String> entry : data.dataInputs.entrySet()) {
			  String key = entry.getKey();
			  String value = entry.getValue();
			  LOGGER.debug("dataInput key:" + key);
			  LOGGER.debug("dataInput value:" + value);			  
		}
		
		
	    String result = esHandler.handle(data);
	    LOGGER.debug("Result is" + result);
	    //TODO Remove System.out
	    
	    // Set the response based on the service retrieved
		return result;
		

	}
	
}
