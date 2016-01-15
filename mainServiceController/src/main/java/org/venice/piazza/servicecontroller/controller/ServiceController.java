package org.venice.piazza.servicecontroller.controller;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.venice.piazza.servicecontroller.CoreServiceProperties;
import org.venice.piazza.servicecontroller.data.model.ExecuteServiceMessage;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;

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
	private final static Logger LOGGER = Logger.getLogger(ServiceController.class);
	/**
	 *  Initialize the handler to handle calls
	 */
	@PostConstruct
	public void initialize() {
		rsHandler = new RegisterServiceHandler(accessor, coreServiceProp);
		esHandler = new ExecuteServiceHandler(accessor, coreServiceProp);
	}
	@RequestMapping(value = "/registerService", method = RequestMethod.POST, headers="Accept=application/json", produces="application/json")
	public @ResponseBody String registerService(@RequestBody ResourceMetadata serviceMetadata) {

		LOGGER.info("serviceMetadata received is " + serviceMetadata);
		System.out.println("serviceMetadata received is " + serviceMetadata);
	    String result = rsHandler.handle(serviceMetadata);
	    
	    System.out.println("ServiceController: Result is" + "{\"resourceId:" + "\"" + result + "\"}");
	    String responseString = "{\"resourceId:\"" + "\"" + result + "\"}";
	    
		return responseString;

	}
	
	@RequestMapping(value = "/executeService", method = RequestMethod.POST, headers="Accept=application/json")
	public @ResponseBody ResponseEntity<?> executeService(@RequestBody ExecuteServiceData data) {
		LOGGER.info("executeService resourceId=" + data.resourceId);
		System.out.println("executeService resourceId=" + data.resourceId);
		LOGGER.info("executeService datainput=" + data.dataInput);
		System.out.println("executeService dataInput =" + data.dataInput);

		for (Map.Entry<String,String> entry : data.dataInputs.entrySet()) {
			  String key = entry.getKey();
			  String value = entry.getValue();
			  LOGGER.info("dataInput key:" + key);
			  LOGGER.info("dataInput value:" + value);
			  System.out.println("dataInput key:" + key);
			  System.out.println("dataInput value:" + value);			  
		}
		
		
	    String result = esHandler.handle(data);
	    LOGGER.info("Result is" + result);
	    //TODO Remove System.out
	    System.out.println("Result is "+result);
	    
	    // Set the response based on the service retrieved
		return new ResponseEntity<>("completed", HttpStatus.OK);
		

	}
	
}
