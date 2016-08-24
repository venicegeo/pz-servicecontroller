/**
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
 **/
package org.venice.piazza.servicecontroller.messaging;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoInterruptedException;

import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.GeoJsonDataType;
import model.data.type.RasterDataType;
import model.data.type.TextDataType;
import model.data.type.URLParameterDataType;
import model.job.Job;
import model.job.PiazzaJobType;
import model.job.result.type.DataResult;
import model.job.result.type.ErrorResult;
import model.job.type.ExecuteServiceJob;
import model.job.type.IngestJob;
import model.request.PiazzaJobRequest;
import model.response.EventTypeListResponse;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import model.workflow.Event;
import model.workflow.EventType;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * 
 * @author mlynum & Sonny.Saniev
 *
 */
@Component
public class ServiceMessageWorker {

	@Value("${SPACE}")
	private String SPACE;

	@Value("${workflow.url}")
	private String WORKFLOW_URL;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private UUIDFactory uuidFactory;

	@Autowired
	private MongoAccessor accessor;

	@Autowired
	private PiazzaLogger coreLogger;

	@Autowired
	private ExecuteServiceHandler esHandler;

	private RestTemplate restTemplate = new RestTemplate();

	/**
	 * Handles service job requests on a thread
	 */
	@Async
	public Future<String> run(ConsumerRecord<String, String> consumerRecord, Producer<String, String> producer, Job job,
			WorkerCallback callback) {
		try {
			String executeJobStatus = StatusUpdate.STATUS_SUCCESS;
			String handleTextUpdate = "";
			ResponseEntity<String> externalServiceResponse = null;
			int statusCode = 400;

			// Ensure a valid Job has been received through Kafka
			if (job == null) {
				throw new Exception("A Null Job has been received by the Service Controller Worker.");
			}

			// Ensure the Job Type is of Execute Service Job
			if ((job.getJobType() == null) || (job.getJobType() instanceof ExecuteServiceJob == false)) {
				throw new Exception("An Invalid Job Type has been received by the Service Controller Worker.");
			}

			// Process the Execution of the External Service
			try {
				PiazzaJobType jobType = job.getJobType();

				coreLogger.log("ExecuteServiceJob Detected with ID " + job.getJobId(), PiazzaLogger.DEBUG);

				// Get the ResourceMetadata
				ExecuteServiceJob jobItem = (ExecuteServiceJob) jobType;
				ExecuteServiceData esData = jobItem.data;
				if (esData.dataOutput != null) {
					DataType dataType = esData.dataOutput.get(0);

					if (Thread.interrupted()) {
						throw new InterruptedException();
					}

					if ((dataType != null) && (dataType instanceof RasterDataType)) {
						// Call special method to call and send
						handleRasterType(jobItem, job, producer);

						// No more to do. Return.
						return new AsyncResult<String>("ServiceMessageWorker_Thread");
					} else {
						coreLogger.log("ExecuteServiceJob Original Way", PiazzaLogger.DEBUG);
						// Execute the external Service and get the Response Entity
						try {
							externalServiceResponse = esHandler.handle(job);
						} catch (MongoInterruptedException exception) {
							// Mongo implements a thread interrupted check, but it doesn't throw an InterruptedException. It throws
							// its own custom exception type. We will catch that exception type here, and then rethrow with a standard
							// InterruptedException to ensure a common handled exception type.
							throw new InterruptedException();
						}

						if (Thread.interrupted()) {
							throw new InterruptedException();
						}

						// If an internal error occurred during Service Handling, then throw an exception.
						if (externalServiceResponse.getStatusCode().is2xxSuccessful() == false) {
							throw new Exception(String.format("Error %s with Status Code %s", externalServiceResponse.getBody(),
									externalServiceResponse.getStatusCode().toString()));
						}

						// If the Response was null, create an empty Response placeholder
						externalServiceResponse = externalServiceResponse != null ? externalServiceResponse
								: new ResponseEntity<String>("", HttpStatus.NO_CONTENT);

						// Process the Response and handle any Ingest that may result
						String dataId = uuidFactory.getUUID();
						DataResult result = processExecutionResult(job, producer, executeJobStatus, externalServiceResponse, dataId);

						if (Thread.interrupted()) {
							throw new InterruptedException();
						}

						// If there is a Result, Send the Status Update with Result to the Job Manager component
						if (result != null) {
							StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
							statusUpdate.setResult(result);
							ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(),
									statusUpdate, SPACE);
							producer.send(prodRecord);
						}

						// Fire Event to Workflow
						fireWorkflowEvent(job.getCreatedBy(), job.getJobId(), dataId, "Service completed successfully.");

						// Return.
						return new AsyncResult<String>("ServiceMessageWorker_Thread");
					}
				} else {
					externalServiceResponse = new ResponseEntity<>(
							"DataOuptut mimeType was not specified.  Please refer to the API for details.", HttpStatus.BAD_REQUEST);
				}
			} catch (IOException | ResourceAccessException ex) {
				coreLogger.log(ex.getMessage(), PiazzaLogger.ERROR);
				executeJobStatus = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = ex.getMessage();
			} catch (HttpClientErrorException | HttpServerErrorException hex) {
				coreLogger.log(hex.getMessage(), PiazzaLogger.ERROR);
				executeJobStatus = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = hex.getResponseBodyAsString();
				statusCode = hex.getStatusCode().value();
			}

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			// If there was no result set then use the default error messages set.
			if (externalServiceResponse == null) {
				sendErrorStatus(executeJobStatus, handleTextUpdate, statusCode, producer, job.getJobId());
			} else {
				// If the status is not OK and the job is not null
				// then send an update to the job manager that there was some failure
				boolean eResult = (externalServiceResponse.getStatusCode() != HttpStatus.OK) ? true : false;
				if (eResult) {
					externalServiceResponse = externalServiceResponse != null ? externalServiceResponse
							: new ResponseEntity<String>("", HttpStatus.NO_CONTENT);
					sendErrorStatus(StatusUpdate.STATUS_FAIL, externalServiceResponse,
							new Integer(externalServiceResponse.getStatusCode().value()), producer, job.getJobId());
				}
			}

		} catch (InterruptedException ex) {
			coreLogger.log(String.format("Thread for Job %s was interrupted.", job.getJobId()), PiazzaLogger.INFO);
			// No need to update Status in this case. Job Manager has done that already at this point.
		} catch (Exception ex) {
			// Catch any General Exceptions that occur during runtime.
			coreLogger.log(ex.getMessage(), PiazzaLogger.ERROR);
			sendErrorStatus(StatusUpdate.STATUS_ERROR, "Unexpected Error in processing External Service: " + ex.getMessage(),
					HttpStatus.INTERNAL_SERVER_ERROR.value(), producer, job.getJobId());
		}

		// Return Future
		return new AsyncResult<String>("ServiceMessageWorker_Thread");
	}

	/**
	 * Sends an error Status to the Job Manager.
	 * 
	 * @param status
	 *            The status. Corresponds with StatusUpdate constants.
	 * @param message
	 *            The message, detailing the error.
	 * @param statusCode
	 *            The numeric HTTP status code.
	 */
	private void sendErrorStatus(String status, Object message, Integer statusCode, Producer<String, String> producer, String jobId) {
		StatusUpdate statusUpdate = new StatusUpdate();
		statusUpdate.setStatus(status);
		// Create a text result and update status
		ErrorResult errorResult = new ErrorResult();
		errorResult.setMessage(message);
		errorResult.setStatusCode(statusCode);
		statusUpdate.setResult(errorResult);

		try {
			ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(
					String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), jobId,
					objectMapper.writeValueAsString(statusUpdate));
			producer.send(prodRecord);
		} catch (JsonProcessingException exception) {
			// The message could not be serialized. Record this.
			exception.printStackTrace();
			coreLogger.log("Could not send Error Status to Job Manager. Error serializing Status: " + exception.getMessage(),
					PiazzaLogger.ERROR);
		}
	}

	/**
	 * Fires the event to the Workflow service that a Service has completed execution.
	 */
	private void fireWorkflowEvent(String user, String jobId, String dataId, String message) {
		coreLogger.log("Firing Event for Completion of ExecuteServiceJob execution", PiazzaLogger.DEBUG);
		try {
			// Retrieve piazza:executionCompletion EventTypeId from pz-workflow.
			String url = String.format("%s/%s?name=%s", WORKFLOW_URL, "eventType", "piazza:executionComplete");
			EventType eventType = objectMapper.readValue(restTemplate.getForObject(url, String.class), EventTypeListResponse.class).data
					.get(0);

			// Construct Event object
			Map<String, Object> data = new HashMap<String, Object>();
			data.put("jobId", jobId);
			data.put("status", message);
			data.put("dataId", dataId);

			Event event = new Event();
			event.createdBy = user;
			event.eventTypeId = eventType.eventTypeId;
			event.data = data;

			// Call pz-workflow endpoint to fire Event object
			restTemplate.postForObject(String.format("%s/%s", WORKFLOW_URL, "event"), objectMapper.writeValueAsString(event), String.class);
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			coreLogger.log(String.format("Could not successfully send Event to Workflow Service. Returned with code %s and message %s",
					exception.getStatusCode().toString(), exception.getResponseBodyAsString()), PiazzaLogger.ERROR);
		} catch (IOException exception) {
			coreLogger.log(String.format("Could not send Event to Workflow Service. Serialization of Event failed with Error: %s",
					exception.getMessage()), PiazzaLogger.ERROR);
		}
	}

	/**
	 * Processes the Result of the external Service execution. This will send the Ingest job through Kafka, and will
	 * return the Result of the data.
	 */
	private DataResult processExecutionResult(Job job, Producer<String, String> producer, String status,
			ResponseEntity<String> handleResult, String dataId) throws JsonProcessingException, IOException, InterruptedException {
		coreLogger.log("Send Execute Status Kafka", PiazzaLogger.DEBUG);
		// Initialize ingest job items
		DataResource data = new DataResource();
		PiazzaJobRequest jobRequest = new PiazzaJobRequest();
		IngestJob ingestJob = new IngestJob();

		if (handleResult != null) {
			coreLogger.log("The result provided from service is " + handleResult.getBody(), PiazzaLogger.DEBUG);

			// String serviceControlString = handleResult.getBody().get(0).toString();
			String serviceControlString = handleResult.getBody().toString();

			PiazzaJobType jobType = job.getJobType();
			ExecuteServiceJob jobItem = (ExecuteServiceJob) jobType;
			String type = jobItem.data.dataOutput.get(0).getClass().getSimpleName();
			coreLogger.log("The service controller string is " + serviceControlString, PiazzaLogger.DEBUG);

			try {
				// Now produce a new record
				jobRequest.createdBy = "pz-sc-ingest";
				data.dataId = dataId;
				coreLogger.log("dataId is " + data.dataId, PiazzaLogger.DEBUG);

				data = objectMapper.readValue(serviceControlString, DataResource.class);

				// Now check to see if the conversion is actually a proper DataResource
				// if it is not time to create a TextDataType and return
				if ((data == null) || (data.getDataType() == null)) {
					coreLogger.log("The DataResource is not in a valid format, creating a new DataResource and TextDataType",
							PiazzaLogger.DEBUG);

					data = new DataResource();
					data.dataId = dataId;
					TextDataType tr = new TextDataType();
					tr.content = serviceControlString;
					coreLogger.log("The data being sent is " + tr.content, PiazzaLogger.DEBUG);

					data.dataType = tr;
				}

			} catch (Exception ex) {
				coreLogger.log(ex.getMessage(), PiazzaLogger.ERROR);

				// Checking payload type and settings the correct type
				if (type.equals((new TextDataType()).getClass().getSimpleName())) {
					TextDataType newDataType = new TextDataType();
					newDataType.content = serviceControlString;
					data.dataType = newDataType;
				} else if (type.equals((new GeoJsonDataType()).getClass().getSimpleName())) {
					GeoJsonDataType newDataType = new GeoJsonDataType();
					newDataType.setGeoJsonContent(serviceControlString);
					data.dataType = newDataType;
				}
			}

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			ingestJob.data = data;
			ingestJob.host = true;
			jobRequest.jobType = ingestJob;

			String jobId = uuidFactory.getUUID();
			ProducerRecord<String, String> newProdRecord = JobMessageFactory.getRequestJobMessage(jobRequest, jobId, SPACE);
			producer.send(newProdRecord);

			coreLogger.log(String.format("Sending Ingest Job Id %s for Data Id %s for Data of Type %s", jobId, data.getDataId(),
					data.getDataType().getClass().getSimpleName()), PiazzaLogger.INFO);

			// Return the Result of the Data.
			DataResult textResult = new DataResult(data.dataId);
			return textResult;
		}
		return null;
	}

	/**
	 * This method is for demonstrating ingest of raster data This will be refactored once the API changes have been
	 * communicated to other team members
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
	public void handleRasterType(ExecuteServiceJob executeJob, Job job, Producer<String, String> producer)
			throws InterruptedException, JsonParseException, JsonMappingException, IOException {
		RestTemplate restTemplate = new RestTemplate();
		ExecuteServiceData data = executeJob.data;
		// Get the id from the data
		String serviceId = data.getServiceId();
		Service sMetadata = accessor.getServiceById(serviceId);
		// Default request mimeType application/json
		String requestMimeType = "application/json";
		new LinkedMultiValueMap<String, String>();

		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getUrl());
		Map<String, DataType> postObjects = new HashMap<String, DataType>();
		Iterator<Entry<String, DataType>> it = data.getDataInputs().entrySet().iterator();
		String postString = "";

		while (it.hasNext()) {
			Entry<String, DataType> entry = it.next();
			String inputName = entry.getKey();

			if (entry.getValue() instanceof URLParameterDataType) {
				String paramValue = ((TextDataType) entry.getValue()).getContent();
				if (inputName.length() == 0) {
					builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getUrl() + "?" + paramValue);
				} else {
					builder.queryParam(inputName, paramValue);
				}
			} else if (entry.getValue() instanceof BodyDataType) {
				BodyDataType bdt = (BodyDataType) entry.getValue();
				postString = bdt.getContent();
				requestMimeType = bdt.getMimeType();
			}
			// Default behavior for other inputs, put them in list of objects
			// which are transformed into JSON consistent with default
			// requestMimeType
			else {
				postObjects.put(inputName, entry.getValue());
			}
		}
		if (postString.length() > 0 && postObjects.size() > 0) {
			return;
		} else if (postObjects.size() > 0) {
			try {
				postString = objectMapper.writeValueAsString(postObjects);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		URI url = URI.create(builder.toUriString());
		HttpHeaders headers = new HttpHeaders();

		// Set the mimeType of the request
		MediaType mediaType = createMediaType(requestMimeType);
		headers.setContentType(mediaType);
		// Set the mimeType of the request
		// headers.add("Content-type",
		// sMetadata.getOutputs().get(0).getDataType().getMimeType());

		if (postString.length() > 0) {

			coreLogger.log("The postString is " + postString, PiazzaLogger.DEBUG);

			HttpHeaders theHeaders = new HttpHeaders();
			// headers.add("Authorization", "Basic " + credentials);
			theHeaders.setContentType(MediaType.APPLICATION_JSON);

			// Create the Request template and execute
			HttpEntity<String> request = new HttpEntity<String>(postString, theHeaders);

			coreLogger.log("About to call special service " + url, PiazzaLogger.DEBUG);

			ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			coreLogger.log("The Response is " + response.getBody(), PiazzaLogger.DEBUG);

			String serviceControlString = response.getBody();
			coreLogger.log("Service Control String " + serviceControlString, PiazzaLogger.DEBUG);

			DataResource dataResource = objectMapper.readValue(serviceControlString, DataResource.class);
			coreLogger.log("dataResource type is " + dataResource.getDataType().getClass().getSimpleName(), PiazzaLogger.DEBUG);

			dataResource.dataId = uuidFactory.getUUID();
			coreLogger.log("dataId " + dataResource.dataId, PiazzaLogger.DEBUG);

			PiazzaJobRequest pjr = new PiazzaJobRequest();
			pjr.createdBy = "pz-sc-ingest-raster-test";

			IngestJob ingestJob = new IngestJob();
			ingestJob.data = dataResource;
			ingestJob.host = true;
			pjr.jobType = ingestJob;

			ProducerRecord<String, String> newProdRecord = JobMessageFactory.getRequestJobMessage(pjr, uuidFactory.getUUID(), SPACE);
			producer.send(newProdRecord);

			coreLogger.log("newProdRecord sent " + newProdRecord.toString(), PiazzaLogger.DEBUG);

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			fireWorkflowEvent(job.getCreatedBy(), job.getJobId(), dataResource.dataId, "Service completed successfully.");

			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);

			// Create a text result and update status
			DataResult textResult = new DataResult(dataResource.dataId);
			statusUpdate.setResult(textResult);
			ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate, SPACE);

			producer.send(prodRecord);
			coreLogger.log("prodRecord sent " + prodRecord.toString(), PiazzaLogger.DEBUG);

		}

	}

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
			coreLogger.log("The type is=" + type, PiazzaLogger.DEBUG);
			coreLogger.log("The subtype is=" + subtype, PiazzaLogger.DEBUG);

		} else {
			// Assume there is just a type for the mime, no subtype
			mediaType = new MediaType(mimeType);
		}

		return mediaType;
	}

}