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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.data.type.TextDataType;
import model.job.PiazzaJobType;
import model.job.metadata.ExecuteServiceData;
import model.job.metadata.InputType;
import model.job.metadata.ParamDataItem;
import model.job.metadata.ResourceMetadata;
import model.job.metadata.Service;
import model.job.type.ExecuteServiceJob;
import util.PiazzaLogger;



/**
 * Handler for handling executeService requests.  This handler is used 
 * when execute-service kafka topics are received or when clients utilize the 
 * ServiceController service.
 * @author mlynum
 * @version 1.0
 *
 */

public class ExecuteServiceHandler implements PiazzaJobHandler {

	private MongoAccessor accessor;
	private PiazzaLogger coreLogger;
	private CoreServiceProperties coreServiceProperties;
	
	private RestTemplate template;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteServiceHandler.class);

	

	public ExecuteServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProperties, PiazzaLogger coreLogger) {
		this.accessor = accessor;
		this.coreServiceProperties = coreServiceProperties;
		this.template = new RestTemplate();
		this.coreLogger = coreLogger;
	
	}

    /*
     * Handler for handling execute service requets.  This
     * method will execute a service given the resourceId and return a response to
     * the job manager.
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest ) {
		ExecuteServiceJob job = (ExecuteServiceJob)jobRequest;
		
		LOGGER.debug("Executing a service");
		if (job != null)  {
			// Get the ResourceMetadata
			ExecuteServiceData esData = job.data;

			ResponseEntity<String> handleResult = handle(esData);
			ArrayList<String> resultList = new ArrayList<String>();
			resultList.add(handleResult.getBody());
			ResponseEntity<List<String>> result = new ResponseEntity<List<String>>(resultList,handleResult.getStatusCode());
			

			// TODO Use the result, send a message with the resource ID
			// and jobId
			return result;
				
		
		}
		else {
			return null;
		}
		
	}//handle
	
	/**
	 * Handles requests to execute a service.  T
	 * TODO this needs to change to levarage pz-jbcommon ExecuteServiceMessage
	 * after it builds.
	 * @param message
	 * @return the Response as a String
	 */
	public ResponseEntity<String> handle (ExecuteServiceData data) {
		ResponseEntity<String> responseEntity = null;
		// Get the id from the data
		String serviceId = data.getServiceId();
		Service sMetadata = accessor.getServiceById(serviceId);
		// Now get the mimeType for the request not using for now..
		String requestMimeType = sMetadata.getMimeType();
		MultiValueMap<String, String> map = new LinkedMultiValueMap<String, String>();
	
		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getResourceMetadata().url);
		Set<String> parameterNames = new HashSet<String>();
		if (sMetadata.getInputs() != null && sMetadata.getInputs().size() > 0) {
			for (ParamDataItem pdataItem : sMetadata.getInputs()) {
				if (pdataItem.getInputType().equals(InputType.URLParameter)) {
					parameterNames.add(pdataItem.getName());
				}
			}
		}
		Map<String,Object> postObjects = new HashMap<String,Object>();
		Iterator<Entry<String,Object>> it = data.getDataInputs().entrySet().iterator();
		String postString = "";
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			String inputName = (String)pair.getKey();
			if (parameterNames.contains(inputName)) {
				if (inputName.length() == 0) {
					String paramValue = (String) ((HashMap)(pair.getValue())).get("content");
					builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getResourceMetadata().url + "?" + paramValue);
				}
				if (pair.getValue() instanceof HashMap) {
					String paramValue = (String) ((HashMap)(pair.getValue())).get("content");
					 builder.queryParam(inputName,paramValue);
				}
				else {
					LOGGER.error("URL parameter value has to be specified in TextDataType" );
					//TODO make ResponseEntity with error
					return null;
				}
			}
			else if (((String)pair.getKey()).toUpperCase().contains("BODY")){
				postString = (String) ((HashMap)(pair.getValue())).get("content");
			}
			else {
				postObjects.put(inputName, pair.getValue());
			}
		}
		if (postString.length() > 0 && postObjects.size() > 0) {
			LOGGER.error("String Input not consistent with other Inputs");
			return null;
		}
		else if (postObjects.size() > 0){
			ObjectMapper mapper = new ObjectMapper();
			try {
				postString = mapper.writeValueAsString(postObjects);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		URI url = URI.create(builder.toUriString());
	
		
		if (sMetadata.getResourceMetadata().method.equals("GET")) {
			responseEntity = template.getForEntity(url, String.class);
			
		}
		else {
			HttpHeaders headers = new HttpHeaders();
			
			// Set the mimeType of the request
			MediaType mediaType = createMediaType(sMetadata.getMimeType());
			headers.setContentType(mediaType);
			HttpEntity<String> requestEntity = null;
			if (postString.length() > 0) {
				requestEntity = this.buildHttpEntity(sMetadata, headers, postString);
				
			}
			else {
				requestEntity = new HttpEntity(headers);
				
			}
			responseEntity = template.postForEntity(url, requestEntity, String.class);
			
		}
		
		
		
		
		
       
	 
	  	return responseEntity;
		
	}
	
	/**
	 * This method creates a MediaType based on the mimetype that was 
	 * provided
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
			
		    subtype = sb.substring(index+1, mimeType.length());
		    mediaType = new MediaType(type, subtype);
		    LOGGER.debug("The type is="+type);
			LOGGER.debug("The subtype="+subtype);
		}
		else {
			// Assume there is just a type for the mime, no subtype
			mediaType = new MediaType(mimeType);			
		}
		
		return mediaType;
	
		
	}
	
	public HttpEntity<String> buildHttpEntity(Service sMetadata, MultiValueMap<String, String> headers, String data) {
	
		
		// Set the mimeType of the request
		headers.add("Content-type", sMetadata.getMimeType());
		//MediaType mediaType = createMediaType(rMetadata.requestMimeType);
		//headers.setContentType(mediaType);
		LOGGER.debug("data to be used " + data);
		LOGGER.debug("Mimetype is " + sMetadata.getMimeType());
		HttpEntity<String> requestEntity = new HttpEntity<String>(data,headers);
		return requestEntity;
	
	}

}