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
import org.venice.piazza.servicecontroller.data.model.CoreResource;
import org.venice.piazza.servicecontroller.data.model.DBCoreResource;
import org.venice.piazza.servicecontroller.data.model.KafkaCoreResource;
import org.venice.piazza.servicecontroller.data.model.RegisterService;




public class CoreInitDestroy implements InitializingBean {
	
	 final static Logger LOGGER = LoggerFactory.getLogger(CoreInitDestroy.class);
	 
	 @Autowired
	 private CoreServiceProperties coreServiceProperties;
	 private String discoverService;
	 private String appName;
	 private String url;
	 private String host;
	 private String discoverAPI;

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
				// It wasn't found so now it's time to register with a put
				registerSuccessful = registerService(false);
			}
		    
			
		}
		// If the registration was unsuccessful, then don't 
		// bother with trying to get other attributes
		if (!registerSuccessful) {
			LOGGER.info(appName + " did not successfully register with the discover service, defaulting to application.property settings");
		}
		
		// Try to Get the other values and set the properties appropriately
		getSupportServiceInfo();
		
	 }
	 
	 /**
	  * obtain properties and check to make sure they are set.
	  */
	 private void obtainProperties() throws IllegalStateException{
		 discoverService = coreServiceProperties.getDiscoverservice();
		 appName = coreServiceProperties.getAppname();
		 LOGGER.debug("DISCOVER = " + discoverService);

		 LOGGER.debug("APPNAME = " + appName);
		 host = coreServiceProperties.getHost();
		 LOGGER.debug("host = " + host);
		 
		 discoverAPI = coreServiceProperties.getDiscoveryapi();
		 LOGGER.info("discoveryAPI = " + discoverAPI);
		 
		 url = host + ":" + coreServiceProperties.getPort();
			 
		 if ((discoverService == null ) && (discoverService.length() < 1)) {
			 throw new IllegalStateException("Property core.discoverservice has not been set");
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
	 
	 private void getSupportServiceInfo () {
		 
		 getMongoHost();
		 getKafkaHost();
		 getUUIDHost();
		 getLoggerHost();
		 
	 }
	 
	 private void getKafkaHost() {

		 RestTemplate template = new RestTemplate();
		 LOGGER.info("About to find the Kafka URL");
		 
		 String kafkaResourceName = coreServiceProperties.getKafka();
		 
		 if (kafkaResourceName != null ) {
		
				 ResponseEntity<KafkaCoreResource> response = null;
				 try {	         
			         
			         // Send the request as GET
			         response = template.getForEntity("http://" + discoverService + discoverAPI + kafkaResourceName, KafkaCoreResource.class);

			         HttpStatus status = response.getStatusCode();
			         
			         LOGGER.info("response is = " + response.getStatusCode().toString());
			         if (status == HttpStatus.OK) {
			        	 KafkaCoreResource cr = response.getBody();
			        	 
			        	 // Split out Port and Host
			        	 StringBuffer sBuffer = new StringBuffer(cr.getHost());
			        	 if (sBuffer != null ) {
			        		 int portIndex = sBuffer.indexOf(":");
			        		 if (portIndex != -1) {
			        			 coreServiceProperties.setKafkaHost(sBuffer.substring(0, portIndex));
			        			 coreServiceProperties.setKafkaPort(new Integer(sBuffer.substring(portIndex + 1)).intValue());
			        			 
			        			 LOGGER.debug("KafkaHost=" + coreServiceProperties.getKafkaHost());
			        			 LOGGER.debug("KafkaPort=" + coreServiceProperties.getKafkaPort());
			        		 } else {
			        			 coreServiceProperties.setKafkaHost(cr.getHost());
			        			 LOGGER.debug("KafkaHost=" + coreServiceProperties.getKafkaHost());
			        		 }
			        		
			        	 }
			        	 
			         }
			        	
			     } catch (HttpClientErrorException ex) {
					HttpStatus status = ex.getStatusCode();
					LOGGER.info("Defaulting to default kafka values " + status.toString());
				    
					
				}
		 } else
			 LOGGER.info("core.kafka is not set, defaulting to kafka application.properties values");
		 
	 }
	 
	 private void getMongoHost() {

		 RestTemplate template = new RestTemplate();
		 LOGGER.info("About to find the MongoDB URL");
		 
		 String mongoDBResourceName = coreServiceProperties.getDb();
		 
		 if (mongoDBResourceName != null ) {
		
				 ResponseEntity<DBCoreResource> response = null;
				 try {	           
		
			         // Send the request as GET
			         response = template.getForEntity("http://" + discoverService + discoverAPI + mongoDBResourceName, DBCoreResource.class);

			         HttpStatus status = response.getStatusCode();
			         
			         LOGGER.info("response is = " + response.getStatusCode().toString());
			         if (status == HttpStatus.OK) {
			        	 DBCoreResource cr = response.getBody();
			        	 
			        	 // Split out Port and Host
			        	 StringBuffer sBuffer = new StringBuffer(cr.getHost());
			        	 if (sBuffer != null ) {
			        		 int portIndex = sBuffer.indexOf(":");
			        		 if (portIndex != -1) {
			        			 coreServiceProperties.setMongoHost(sBuffer.substring(0, portIndex));
			        			 coreServiceProperties.setMongoPort(new Integer(sBuffer.substring(portIndex + 1)).intValue());
			        			 LOGGER.debug("MongoHost=" + coreServiceProperties.getMongoHost());
			        			 LOGGER.debug("MongoPort=" + coreServiceProperties.getMongoPort());
			        		 } else {
			        			 coreServiceProperties.setMongoHost(cr.getHost());
			        			 LOGGER.debug("MongoHost=" + coreServiceProperties.getMongoHost());
			        		 }
			        	 }
			        	 
			         }
			        	
			     } catch (HttpClientErrorException ex) {
					HttpStatus status = ex.getStatusCode();
					LOGGER.info("Defaulting to default mongodb values " + status.toString());
				    
					
				}
		 } else
			 LOGGER.info("core.db is not set, defaulting to mongodb application.properties values");
		 
	 }
	 
	 private void getUUIDHost() {

		 RestTemplate template = new RestTemplate();
		 LOGGER.info("About to find the pz-uuidgen URL");
		 
		 String uuidResourceName = coreServiceProperties.getUuid();
		 
		 if (uuidResourceName != null ) {
		
				 ResponseEntity<CoreResource> response = null;
				 try {	         
			         
			         // Send the request as GET
			         response = template.getForEntity("http://" + discoverService + discoverAPI + uuidResourceName, CoreResource.class);

			         HttpStatus status = response.getStatusCode();
			         
			         LOGGER.info("response is = " + response.getStatusCode().toString());
			         if (status == HttpStatus.OK) {
			        	 CoreResource cr = response.getBody();
			        	 
			        	 // Split out Port and Host
			        	 coreServiceProperties.setUuidservice(cr.getAddress());
			        	 
			         }
			        	
			     } catch (HttpClientErrorException ex) {
					HttpStatus status = ex.getStatusCode();
					LOGGER.info("Defaulting to default pz-uuidgen values " + status.toString());
				    
					
				}
		 } else
			 LOGGER.info("core.uuid is not set, defaulting to uuidgen application.properties values");
		 
	 }
	 
	 private void getLoggerHost() {

		 RestTemplate template = new RestTemplate();
		 obtainProperties();
		 LOGGER.info("About to find the pz-logger URL");
		 
		 String loggerResourceName = coreServiceProperties.getLogger();
		 
		 if (loggerResourceName != null ) {
		
				 ResponseEntity<CoreResource> response = null;
				 try {	         
			         
			         // Send the request as GET
			         response = template.getForEntity("http://" + discoverService + discoverAPI + loggerResourceName, CoreResource.class);

			         HttpStatus status = response.getStatusCode();
			         
			         LOGGER.info("response is = " + response.getStatusCode().toString());
			         if (status == HttpStatus.OK) {
			        	 CoreResource cr = response.getBody();
			        	 
			        	 // Split out Port and Host
			        	 coreServiceProperties.setLogservice(cr.getAddress());
			        	 
			         }
			        	
			     } catch (HttpClientErrorException ex) {
					HttpStatus status = ex.getStatusCode();
					LOGGER.info("Defaulting to default logservice values " + status.toString());
				    
					
				}
		 } else
			 LOGGER.info("core.uuid is not set, defaulting to logservice application.properties values");
		 
	 }
	 
}