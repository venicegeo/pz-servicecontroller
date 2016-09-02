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
package org.venice.piazza.servicecontroller.messaging.handlers;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.URLParameterDataType;
import model.job.PiazzaJobType;
import model.job.type.ExecuteServiceJob;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import util.PiazzaLogger;

/**
 * Handler for handling executeService requests.  This handler is used 
 * when execute-service kafka topics are received or when clients utilize the 
 * ServiceController service.
 * @author mlynum & Sonny.Saniev
 * @version 1.0
 */
@Component
public class ExecuteServiceHandler implements PiazzaJobHandler {

	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private PiazzaLogger coreLogger;

	private RestTemplate template = new RestTemplate();
	
    /**
     * Handler for handling execute service requests. This method will execute a service given 
     * the resourceId and return a response to the job manager.
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	@Override
	public ResponseEntity<String> handle (PiazzaJobType jobRequest ) {
		coreLogger.log("Executing a Service.", PiazzaLogger.DEBUG);


		ExecuteServiceJob job = (ExecuteServiceJob)jobRequest;
		// Check to see if this is a valid request
		if (job != null)  {
			// Get the ResourceMetadata
			ExecuteServiceData esData = job.data;
			ResponseEntity<String> handleResult = handle(esData);
			ResponseEntity<String> result = new ResponseEntity<>(handleResult.getBody(), handleResult.getStatusCode());
			coreLogger.log("The result is " + result, PiazzaLogger.DEBUG);
			
			// TODO Use the result, send a message with the resource Id and jobId
			return result;
		}
		else {
			coreLogger.log("Job is null", PiazzaLogger.ERROR);
			return new ResponseEntity<>("Job is null", HttpStatus.BAD_REQUEST);
		}
	}

	/**
	 * Handles requests to execute a service. 
	 * TODO this needs to change to leverage pz-jbcommon ExecuteServiceMessage after it builds.
	 * 
	 * @param message
	 * @return the Response as a String
	 */
	public ResponseEntity<String> handle(ExecuteServiceData data) {
		coreLogger.log(String.format("Beginning execution of Service ID %s", data.getServiceId()), PiazzaLogger.INFO);
		ResponseEntity<String> responseEntity = null;
		String serviceId = data.getServiceId();
		Service sMetadata = null;
	 	// Default request mimeType application/json
		String requestMimeType = "application/json";
		try {
			// Accessor throws exception if can't find service
			 sMetadata= accessor.getServiceById(serviceId);
	
			ObjectMapper om = new ObjectMapper();
		    String result = om.writeValueAsString(sMetadata);
		    coreLogger.log(result, PiazzaLogger.INFO);
		} catch (ResourceAccessException | JsonProcessingException ex) {
			ex.printStackTrace();
		}
		if (sMetadata != null) {
			String rawURL = sMetadata.getUrl();
		    coreLogger.log(String.format("Executing Service with URL %s with ID %s", rawURL, serviceId),  PiazzaLogger.INFO);
			UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(rawURL);
	
			Map<String, DataType> postObjects = new HashMap<>();
			Iterator<Entry<String, DataType>> it = data.getDataInputs().entrySet().iterator();
			String postString = "";
			while (it.hasNext()) {
				Entry<String, DataType> entry = it.next();
				String inputName = entry.getKey();
				coreLogger.log("The parameter is " + inputName, PiazzaLogger.DEBUG);
	
	
				if (entry.getValue() instanceof URLParameterDataType) {
					String paramValue = ((URLParameterDataType) entry.getValue()).getContent();
					if (inputName.length() == 0) {
						coreLogger.log("sMetadata.getResourceMeta=" + sMetadata.getResourceMetadata(), PiazzaLogger.DEBUG);
	
	
						builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getUrl() + "?" + paramValue);
						coreLogger.log("Builder URL is " + builder.toUriString(), PiazzaLogger.DEBUG);
	
	
					} else {
						builder.queryParam(inputName, paramValue);
						coreLogger.log("Input Name=" + inputName + " paramValue=" + paramValue, PiazzaLogger.DEBUG);
					}
				} else if (entry.getValue() instanceof BodyDataType) {
					BodyDataType bdt = (BodyDataType) entry.getValue();
					postString = bdt.getContent();
					requestMimeType = bdt.getMimeType();
					if ((requestMimeType == null) || (requestMimeType.length() == 0)) {
						coreLogger.log("Body mime type not specified", PiazzaLogger.ERROR);
						return new ResponseEntity<>("Body mime type not specified", HttpStatus.BAD_REQUEST);
					}
				} else {
					// Default behavior for other inputs, put them in list of objects
					// which are transformed into JSON consistent with default requestMimeType
					coreLogger.log("inputName =" + inputName + "entry Value=" + entry.getValue(), PiazzaLogger.INFO);
					postObjects.put(inputName, entry.getValue());
				}
			}
	
			coreLogger.log("Final Builder URL" + builder.toUriString(), PiazzaLogger.INFO);
			if (postString.length() > 0 && postObjects.size() > 0) {
				coreLogger.log("String Input not consistent with other Inputs", PiazzaLogger.ERROR);
				return new ResponseEntity<>("String Input not consistent with other Inputs", HttpStatus.BAD_REQUEST);
			} else if (postObjects.size() > 0) {
				ObjectMapper mapper = makeObjectMapper();
				try {
					postString = mapper.writeValueAsString(postObjects);
				} catch (JsonProcessingException e) {
					coreLogger.log(e.getMessage(), PiazzaLogger.ERROR);
					return new ResponseEntity<>("Could not marshal post requests", HttpStatus.BAD_REQUEST);
				}
			}
			
			URI url = URI.create(builder.toUriString());

			// Setting timeout for HTTP requests
			if (null != sMetadata.getTimeout() && sMetadata.getTimeout() > 0) {
				HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
				factory.setReadTimeout(sMetadata.getTimeout().intValue());
				factory.setConnectTimeout(sMetadata.getTimeout().intValue());
				template = new RestTemplate(factory);
			}
			
			if (sMetadata.getMethod().equals("GET")) {
				coreLogger.log("GetForEntity URL=" + url, PiazzaLogger.INFO);
				// execute job
				responseEntity = template.getForEntity(url, String.class);
			} else {
				HttpHeaders headers = new HttpHeaders();
				// Set the mimeType of the request
				MediaType mediaType = createMediaType(requestMimeType);
				headers.setContentType(mediaType);
				HttpEntity<String> requestEntity = makeHttpEntity(headers, postString);

				coreLogger.log("PostForEntity URL=" + url, PiazzaLogger.INFO);
				responseEntity = template.postForEntity(url, requestEntity, String.class);
			}
		} else
		{
			return new ResponseEntity<>("Service Id " + data.getServiceId() + " not found", HttpStatus.NOT_FOUND);

		}
		return responseEntity;
	}
	
	/**
	 * This method creates a MediaType based on the mimetype that was provided
	 * 
	 * @param mimeType
	 * @return MediaType
	 */
	private MediaType createMediaType(String mimeType) {
		MediaType mediaType;
		String type, subtype;
		StringBuffer sb = new StringBuffer(mimeType);
		int index = sb.indexOf("/");
		// If a slash was found then there is a type and subtype
		if (index != -1) {
			type = sb.substring(0, index);

			subtype = sb.substring(index + 1, mimeType.length());
			mediaType = new MediaType(type, subtype);
		} else {
			// Assume there is just a type for the mime, no subtype
			mediaType = new MediaType(mimeType);
		}

		return mediaType;
	}
	
	/**
	 * create HttpEntity
	 */
	public HttpEntity<String> makeHttpEntity(HttpHeaders headers, String postString) {
		HttpEntity<String> requestEntity;

		if (postString.length() > 0)
			requestEntity = new HttpEntity<>(postString, headers);
		else
			requestEntity = new HttpEntity<>(headers);

		return requestEntity;

	}

	ObjectMapper makeObjectMapper() {
		return new ObjectMapper();
	}

}