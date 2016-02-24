/*******************************************************************************
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.venice.piazza.servicecontroller.controller;

import java.util.Map;

import javax.annotation.PostConstruct;

import model.job.metadata.ExecuteServiceData;
import model.job.metadata.ResourceMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.venice.piazza.servicecontroller.data.model.SearchCriteria;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.DeleteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.DescribeServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ListServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.SearchServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.UpdateServiceHandler;

import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import util.PiazzaLogger;
import util.UUIDFactory;

/** 
 * Purpose of this controller is to handle service requests for registerin
 * and managing services.
 * @author mlynum
 * @since 1.0
 */

@RestController

@RequestMapping({"/servicecontroller", ""})
@DependsOn("coreInitDestroy")
public class ServiceController {
	private RegisterServiceHandler rsHandler;
	private ExecuteServiceHandler esHandler;
	private DescribeServiceHandler dsHandler;
	private UpdateServiceHandler usHandler;
	private ListServiceHandler lsHandler;
	private DeleteServiceHandler dlHandler;
	private SearchServiceHandler ssHandler;
	
	@Autowired
	private MongoAccessor accessor;
	
	@Autowired
	private CoreServiceProperties coreServiceProp;
	
	@Autowired
	private PiazzaLogger logger;
	
	@Autowired
	private UUIDFactory uuidFactory;
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceController.class);
	
	public ServiceController() {
		
	}
	/**
	 *  Initialize the handler to handle calls
	 */
	@PostConstruct
	public void initialize() {
		
		// Initialize calling server
		rsHandler = new RegisterServiceHandler(accessor, coreServiceProp, logger, uuidFactory);
		usHandler = new UpdateServiceHandler(accessor, coreServiceProp, logger, uuidFactory);
		esHandler = new ExecuteServiceHandler(accessor, coreServiceProp, logger);
		dsHandler = new DescribeServiceHandler(accessor, coreServiceProp, logger);
		dlHandler = new DeleteServiceHandler(accessor, coreServiceProp, logger, uuidFactory);
		lsHandler = new ListServiceHandler(accessor, coreServiceProp, logger);
		ssHandler = new SearchServiceHandler(accessor, coreServiceProp, logger);

	
	}
	/**
	 * Registers a service with the piazza service controller. 
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration
	 * and for testing of the serviceController.  
	 * @param serviceMetadata 
	 * 		     metadata about the service
	 * @return A Json message with the resourceID {resourceId="<the id>"}
	 */
	@RequestMapping(value = "/registerService", method = RequestMethod.POST, headers="Accept=application/json", produces=MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String registerService(@RequestBody ResourceMetadata serviceMetadata) {

		LOGGER.debug("serviceMetadata received is " + serviceMetadata);
	    String result = rsHandler.handle(serviceMetadata);
	    
	    LOGGER.debug("ServiceController: Result is" + "{\"resourceId:" + "\"" + result + "\"}");
	    String responseString = "{\"resourceId\":" + "\"" + result + "\"}";
	    
		return responseString;

	}
	
	/**
	 * Updates metadata about an existing service registered in the ServiceController.
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration
	 * and for testing of the serviceController. 
	 *  
	 * @param serviceMetadata
	 * 		  metadata bout the service
	 * @return A Json message with the resourceID {resourceId="<the id>"}
	 */
	@RequestMapping(value = "/updateService", method = RequestMethod.PUT, headers="Accept=application/json", produces=MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String updateService(@RequestBody ResourceMetadata serviceMetadata) {

		LOGGER.debug("serviceMetadata received is " + serviceMetadata);
	    String result = usHandler.handle(serviceMetadata);
	    
	    LOGGER.debug("ServiceController: Result is" + "{\"resourceId:" + "\"" + result + "\"}");
	    String responseString = "{\"resourceId\":" + "\"" + result + "\"}";
	    
		return responseString;

	}
	/**
	 * Executes a service registered in the Service Controller.
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration
	 * and for testing of the serviceController.  
	 * 
	 * @param data
	 * 			ExecuteServiceData used to execute the data.  Contains resourceId and
	 *          values to use.
	 *          
	 * @return the  results of the service execution
	 */
	@RequestMapping(value = "/executeService", method = RequestMethod.POST, headers="Accept=application/json")
	public ResponseEntity<String> executeService(@RequestBody ExecuteServiceData data) {
		LOGGER.debug("executeService resourceId=" + data.resourceId);
		LOGGER.debug("executeService datainput=" + data.dataInput);

		for (Map.Entry<String,String> entry : data.dataInputs.entrySet()) {
			  String key = entry.getKey();
			  String value = entry.getValue();
			  LOGGER.debug("dataInput key:" + key);
			  LOGGER.debug("dataInput value:" + value);			  
		}
		
		
	    ResponseEntity<String> result = esHandler.handle(data);
	    LOGGER.debug("Result is" + result);
	    //TODO Remove System.out
	    
	    // Set the response based on the service retrieved
		return result;
		

	}
	/**
	 * Used to describe details about the service.
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration
	 * and for testing of the serviceController.  
	 * 
	 * @param resourceId
	 * 			The id associated with the service that is registered within the
	 * 			Service Controller.
	 * @return Json with the ResourceMetadata, the metadata about the service
	 */
	@RequestMapping(value = "/describeService", method = RequestMethod.GET, headers="Accept=application/json")
	public ResponseEntity<String> describeService(@ModelAttribute("resourceId") String resourceId) {
		LOGGER.debug("describeService resourceId=" + resourceId);
	
			
	    ResponseEntity<String> result = dsHandler.handle(resourceId);
	    LOGGER.debug("Result is" + result);
	    //TODO Remove System.out
	    
	    // Set the response based on the service retrieved
		return result;
		

	}
	
	/**
	 * deletes a registered service from the ServiceController.  
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration
	 * and for testing of the serviceController.  
	 * 
	 * @param resourceId
	 * @return the result of the deletion
	 */
	@RequestMapping(value = "/deleteService", method = RequestMethod.GET, headers="Accept=application/json")
	public ResponseEntity<String> deleteService(@ModelAttribute("resourceId") String resourceId) {
		LOGGER.debug("deleteService resourceId=" + resourceId);
	
			
	    String result = dlHandler.handle(resourceId);
	    LOGGER.debug("Result is" + result);
	    //TODO Remove System.out
	    
	    // Set the response based on the service retrieved
		return new ResponseEntity<String>(result,HttpStatus.OK);
		

	}
	/**
	 * Lists all the services registered in the service controller.
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration
	 * and for testing of the serviceController.  
	 * 
	 * @return Json list o resourceMetadata items (Metadata about the service)
	 */
	@RequestMapping(value = "/listService", method = RequestMethod.GET, headers="Accept=application/json")
	public ResponseEntity<String> listService() {
		
	
			
	    ResponseEntity<String> result = lsHandler.handle();
	    LOGGER.debug("Result is" + result);
	    //TODO Remove System.out
	    
	    // Set the response based on the service retrieved
		return result;
		

	}
	/**
	 * Searches for registered services.
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration
	 * and for testing of the serviceController. 
	 * 
	 * @param SearchCriteria
	 * 			The criteria to search with (specify field and regular expression
	 * 
	 * @return Json list o resourceMetadata items (Metadata about the service)
	 */
	@RequestMapping(value = "/search", method = RequestMethod.POST, headers="Accept=application/json")
	public ResponseEntity<String> search(@RequestBody SearchCriteria criteria) {

	    ResponseEntity<String> result = ssHandler.handle(criteria);
	    LOGGER.debug("Result is" + result);
	    //TODO Remove System.out
	    
	    // Set the response based on the service retrieved
		return result;
		

	}
	
	/**
	 * Healthcheck to see if the Piazza Service Controller is up and running.
	 * his service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration
	 * and for testing of the serviceController. 
	 * 
	 * @return welcome message
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public ResponseEntity<String> healthCheck() {

	    
	    LOGGER.debug("Health Check called");
	    
	    HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(MediaType.valueOf("text/html"));
        String htmlMessage = "<HTML><TITLE>Piazza Service Controller Welcome</TITLE>";
        htmlMessage = htmlMessage +
        		      "<BODY><BR> Welcome from the Piazza Service Controller. " +
        		      "<BR>For details on running and using the ServiceController, " +
        		      "<BR>see <A HREF=\"https://github.com/venicegeo/venice/wiki/Pz-ServiceController\"> Pz Service Controller<A> for details." +
        		      "<BODY></HTML>";
        
	    ResponseEntity<String> response = new ResponseEntity<String>(htmlMessage, responseHeaders, HttpStatus.OK);
	    
	    // Set the response based on the service retrieved
		return response;
		

	}
	
	
	
}
