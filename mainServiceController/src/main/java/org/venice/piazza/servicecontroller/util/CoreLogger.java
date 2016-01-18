package org.venice.piazza.servicecontroller.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.CoreServiceProperties;

// Add License header
/**
 * CoreLogger is a class that logs using the Piazza Core Logger service.
 * @author mlynum
 * @version 1.0
 */

@Component
public class CoreLogger {
	
	public static final String DEBUG="Debug";
	public static final String ERROR="Error";
	public static final String FATAL="Fatal";
	public static final String INFO="Info";
	public static final String WARNING="Warning";
	
	private final static Logger LOGGER = LoggerFactory.getLogger(CoreLogger.class);

	
	private RestTemplate template;
	
	@Autowired
	private CoreServiceProperties coreServiceProp;
	private static String serviceName = "service-controller";
	
	@PostConstruct
	public void init() {
		template = new RestTemplate();
	}

	
	public  void log(String logMessage, String severity) {
		ResponseEntity<String> response = null;
		try {
			// Get the date as UTC
			String currentDate = new DateTime( DateTimeZone.UTC ).toString();
			//TODO Need to verify that this actually returns
			// the correct address.  It may return one of many network interfaces
			String address = InetAddress.getLocalHost().toString();

			// Create a map to send things to the Piazza core log service
			MultiValueMap<String, Object> map = new LinkedMultiValueMap<String, Object>();
			/*map.add("service", serviceName);			
			map.add("address", address);
			map.add("time",currentDate);
			map.add("message", logMessage);
			map.add("severity", severity); */
			
			LOGGER.info("LogService is " + coreServiceProp.getLogservice());
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			
			// Build the log request
			//String responseString = "{\"resourceId\":" + "\"" + result + "\"}";
			String logRequest = "{\"service\":" + "\"" + serviceName + "\"," +
					"\"address\":" + "\"" + address + "\"," +
					"\"time\":" + "\"" + currentDate + "\"," +
					"\"message\":" + "\"" + logMessage + "\"," +
					"\"severity\":" + "\"" + severity + "\"}";
			

			HttpEntity<String> requestEntity = new HttpEntity<String>(logRequest,headers);
		
			response = template.postForEntity("http://" + coreServiceProp.getLogservice(), requestEntity, String.class);
			LOGGER.info("Response is" + response.toString());	
		} catch (UnknownHostException uhe) {
			
			LOGGER.error(uhe.getMessage());
			LOGGER.error("Could not connect to the logging service");			
		}	catch (ResourceAccessException rae) {
			LOGGER.error(rae.getMessage());
			LOGGER.error("Could not connect to the logging service");		
		}
			
			
		
	}

}
