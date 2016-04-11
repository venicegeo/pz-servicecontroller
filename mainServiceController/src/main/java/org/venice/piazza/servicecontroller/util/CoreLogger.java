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
package org.venice.piazza.servicecontroller.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.request.LogRequest;
// Add License header
/**
 * CoreLogger is a class that logs using the Piazza Core Logger service.
 * @author mlynum
 * @version 1.0
 */

@Component
@DependsOn("coreInitDestroy")
public class CoreLogger {
	
	public static final String DEBUG="Debug";
	public static final String ERROR="Error";
	public static final String FATAL="Fatal";
	public static final String INFO="Info";
	public static final String WARNING="Warning";

	private String logService;
	private String logServiceHost;
	private String appname;
	@Autowired
	CoreServiceProperties coreServiceProperties;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(CoreLogger.class);
	private RestTemplate template;
	
	
	@PostConstruct
	public void init() {
		LOGGER.info("CoreLogger initialized");
		template = new RestTemplate();
		logService = coreServiceProperties.getLogservice();
		logServiceHost = coreServiceProperties.getLogservicehost();
		appname = coreServiceProperties.getAppname();
	}

	/**
	 * method for logging messages to Pz-Logger
	 * @param logMessage -  the message you want to log
	 * @param severity - the severity of the log
	 */
	public void log(String logMessage, String severity) {
		ResponseEntity<String> response = null;
		if ((logMessage != null) && (logMessage.length() > 0)) {
			LogRequest logRequest = new LogRequest();
			try {
				// Get the date as UTC
				String currentDate = new DateTime( DateTimeZone.UTC ).toString();
				//TODO Need to verify that this actually returns
				// the correct address.  It may return one of many network interfaces
				String address = InetAddress.getLocalHost().toString();
	
								
				LOGGER.info("LogService Host is " + logServiceHost);
				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON);
				
				// Build the log request JSON
				ObjectMapper mapper = new ObjectMapper();
				
				logRequest.setAddress(address);
				logRequest.setSeverity(severity);
                                // TODO Need to remove this class 

				//logRequest.
				logRequest.setService(appname);
				logRequest.setMessage(logMessage);
				// Now see if the object can be written to a string, if it
				// cant an exception  will happen
				String jsonLogRequest = mapper.writeValueAsString(logRequest);			
	
				HttpEntity<LogRequest> requestEntity = new HttpEntity<LogRequest>(logRequest,headers);
			
				response = template.postForEntity("http://" + logServiceHost + logService, requestEntity, String.class);
				LOGGER.info("Response is" + response.toString());	
			} catch (UnknownHostException uhe) {
				
				LOGGER.error(uhe.getMessage());
				LOGGER.error("Could not connect to the logging service");			
			}	catch (ResourceAccessException rae) {
				LOGGER.error(rae.getMessage());
				LOGGER.error("Could not connect to the logging service");		
			} catch (HttpClientErrorException hce) {
				LOGGER.error("Logger service is not available.  Logger URL is " + logServiceHost);
			} catch (JsonProcessingException ex) {
				LOGGER.error("There was problem creating JSON for the following object " + logRequest.toString());
			}
		}
		else
			LOGGER.info("No message to send, logMessage = " + logMessage + "|");
			
			
		
	}

}
