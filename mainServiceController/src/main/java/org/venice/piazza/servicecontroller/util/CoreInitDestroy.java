package org.venice.piazza.servicecontroller.util;

/**
 * Bean which is responsible for registering with the pz-discover service and initializing properties and 
 * other resources.
 * @author mlynum
 * @version 1.0
 */

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.model.RegisterService;




public class CoreInitDestroy implements InitializingBean {
	
	 final static Logger LOGGER = LoggerFactory.getLogger(CoreInitDestroy.class);
	 private final static String discoverAPI = "/api/v1/resources"; 
	 
	 @Autowired
	 private CoreServiceProperties coreServiceProperties;
	 private String discoverService;
	 private String appName;
	 private String url;
	 private String host;

	/**
	 * Constructor
	 */
	 public CoreInitDestroy() {
		 LOGGER.info("Constructor called");

			 
	 }
	 @PreDestroy
	 /** 
	  * Clean up resources and de-register...
	  * @throws Exception
	  */
	 public void cleanUp() throws Exception {
		 RestTemplate template = new RestTemplate();
		 LOGGER.info("Destroying and de-registering");

		
		 ResponseEntity<String> response = null;
		 try {	         
	         // Prepare header
	         HttpHeaders headers = new HttpHeaders();
	         HttpEntity<String> entity = new HttpEntity<String>(headers);    

	         // Send the request as DELETE
	         response = template.exchange("http://" + discoverService + discoverAPI + "/{name}", HttpMethod.DELETE, entity, String.class, appName);
	         if (response != null)
	        	 LOGGER.info("response is = " + response.getStatusCode().toString());
	         else
	        	 LOGGER.info("Could not de-register from pz-discover." + appName);
	         // ResponseEntity<byte[]> result = restTemplate.exchange("http://localhost:7070/spring-rest-provider/krams/person/{id}", HttpMethod.GET, entity, byte[].class, id);
	     } catch (HttpClientErrorException ex) {
			HttpStatus status = ex.getStatusCode();
			if (status == HttpStatus.NOT_FOUND) {
				LOGGER.info("Could not de-register from pz-discover." + appName + "Not Found");
			}
		    
			
		}
	  
	 }

	 @Override
	 /** 
	  * Set the properties by calling the pz-discover service
	  */
	 public void afterPropertiesSet() throws Exception {
		 boolean registerSuccessful = false;
		 RestTemplate template = new RestTemplate();
		 obtainProperties();
		 LOGGER.info("About to call discovery at " + discoverService);
		
		 ResponseEntity<String> response = null;
		 try {	         
	         // Prepare header
	         HttpHeaders headers = new HttpHeaders();
	         HttpEntity<String> entity = new HttpEntity<String>(headers);    

	         // Send the request as GET
	         response = template.exchange("http://" + discoverService + discoverAPI + "/{app-name}", HttpMethod.GET, entity, String.class, appName);
	         HttpStatus status = response.getStatusCode();
	         
	         LOGGER.info("response is = " + response.getStatusCode().toString());
	         if (status == HttpStatus.OK)
	        	 registerSuccessful = registerService(true);
	     } catch (HttpClientErrorException ex) {
			HttpStatus status = ex.getStatusCode();
			if (status == HttpStatus.NOT_FOUND) {
				// It wasnt found so now it's time to register with a put
				registerSuccessful = registerService(false);
			}
		    
			
		}
		// If the registration was unsuccessful, then don't 
		// bother with trying to get other attributes
		if (!registerSuccessful) {
			LOGGER.info(appName + " did not successfully register with the discover service, defaulting to application.property settings");
		}
		else {
			// Get the other values and set the properties appropriately
		}
	 }
	 
	 /**
	  * obtain properties and check to make sure they are set.
	  */
	 private void obtainProperties() throws IllegalStateException{
		 discoverService = coreServiceProperties.getDiscoveryservice();
		 appName = coreServiceProperties.getAppname();
		 LOGGER.info("DISCOVER = " + discoverService);

		 LOGGER.info("APPNAME = " + appName);
		 host = coreServiceProperties.getHost();
		 LOGGER.info("host = " + host);
		 url = host + ":" + coreServiceProperties.getPort();
			 
		 if ((discoverService == null ) && (discoverService.length() < 1)) {
			 throw new IllegalStateException("Property core.discoveryservice has not been set");
		 }
			 
		 if ((appName == null) && (appName.length() < 1)) {
			 throw new IllegalStateException("Property servicecontroller.appname has not been set");
		 }
		 
		 if ((host == null) && (host.length() < 1)) {
			 throw new IllegalStateException("Property servicecontroller.host and/or serviceController.port has not been set");
		 }
		 
		 
	 }
	 
	 /**
	  * register with the pz-discover service
	  * @param update - is this a new registration or an update
	  * @return true - successful registration, false - unsuccessful registration
	  */
	 private boolean registerService(boolean update) {
		 boolean success = false;
		 ResponseEntity<String> response = null;
		 RestTemplate template = new RestTemplate();
		 
		 try {	         
	         // Prepare header
	         HttpHeaders headers = new HttpHeaders(); 
	         
	         // Create a map to send things to the Piazza core log service
		     Map<String, String> map = new HashMap<String, String>();
		     
			 map.put("type", "core-service");			
			 LOGGER.info("URL IS=" + url + "|");
			 map.put("address", url);
			 headers.setContentType(MediaType.APPLICATION_JSON);
			 RegisterService rs = new RegisterService();
			 rs.setName(appName);
			 rs.setData(map);
		
			 HttpEntity<RegisterService> entity = new HttpEntity<RegisterService>(rs,headers);
			
	         if (update)
	        	 response = template.exchange("http://" + discoverService + discoverAPI, HttpMethod.POST, entity, String.class, appName);
	         else
	        	 response = template.exchange("http://" + discoverService + discoverAPI, HttpMethod.PUT, entity, String.class, appName);

	         HttpStatus status = response.getStatusCode();
	         
	         LOGGER.info("response is = " + response.getStatusCode().toString());
	         if (status == HttpStatus.OK)
	        	success = true;

		} catch (HttpClientErrorException ex) {
			ex.printStackTrace();		    			
		}
		 return success;
		 
	 }
	 
}