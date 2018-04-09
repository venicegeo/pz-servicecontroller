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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.async.AsynchronousServiceWorker;
import org.venice.piazza.servicecontroller.data.accessor.DatabaseAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.taskmanaged.ServiceTaskManager;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import exception.DataInspectException;
import exception.PiazzaJobException;
import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.TextDataType;
import model.data.type.URLParameterDataType;
import model.job.Job;
import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.result.type.DataResult;
import model.job.result.type.ErrorResult;
import model.job.result.type.TextResult;
import model.job.type.ExecuteServiceJob;
import model.job.type.IngestJob;
import model.logger.AuditElement;
import model.logger.Severity;
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
 * Threaded Worker class that handles the logic for executing a service; including invoking asynchronous and
 * task-managed services.
 * 
 * @author mlynum & Sonny.Saniev
 *
 */
@Component
public class
ServiceMessageWorker {
	@Value("${SPACE}")
	private String SPACE;
	@Value("${workflow.url}")
	private String WORKFLOW_URL;

	@Autowired
	private ObjectMapper objectMapper;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private DatabaseAccessor accessor;
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private ExecuteServiceHandler esHandler;
	@Autowired
	private AsynchronousServiceWorker asynchronousServiceWorker;
	@Autowired
	private ServiceTaskManager serviceTaskManager;
	@Autowired
	private RestTemplate restTemplate;
	@Autowired
	@Qualifier("UpdateJobsQueue")
	private Queue updateJobsQueue;
	@Autowired
	@Qualifier("RequestJobQueue")
	private Queue requestJobQueue;
	@Autowired
	private RabbitTemplate rabbitTemplate;

	private static final Logger LOG = LoggerFactory.getLogger(ServiceMessageWorker.class);
	private static final String JSON_ERR = "Json processing error occurred";

	/**
	 * Handles service job requests on a thread
	 */
	@Async
	public Future<String> run(Job job, WorkerCallback callback) {
		String jobId = (job == null) ? "null" : job.getJobId();
		try {
			validateJob(job);

			// Process the Execution of the External Service
			return processExernalServiceExecution(job, callback);
		} catch (InterruptedException ex) { // NOSONAR normal handling of InterruptedException
			interruptJob(jobId, ex.toString());
		} catch (Exception ex) {
			LOG.error("Unexpected Error in processing External Service", ex);
			// Catch any General Exceptions that occur during runtime.
			logger.log(ex.getMessage(), Severity.ERROR);

			sendErrorStatus(StatusUpdate.STATUS_ERROR, "Unexpected Error in processing External Service: " + ex.getMessage(),
					HttpStatus.INTERNAL_SERVER_ERROR.value(), jobId);
		}

		// Return Future
		callback.onComplete(jobId);

		return new AsyncResult<>("ServiceMessageWorker_Thread");
	}

	private void validateJob(final Job job) throws PiazzaJobException, DataInspectException {
		// Ensure a valid Job has been received
		if (job == null) {
			throw new DataInspectException("A Null Job has been received by the Service Controller Worker.");
		}

		// Ensure the Job Type is of Execute Service Job
		if ((job.getJobType() == null) || (job.getJobType() instanceof ExecuteServiceJob == false)) {
			throw new PiazzaJobException("An Invalid Job Type has been received by the Service Controller Worker.",
					HttpStatus.BAD_REQUEST.value());
		}
	}

	private void validateResourceMetadata(final ResourceMetadata rMetadata, final String serviceId) throws DataInspectException {
		if ((rMetadata != null) && (rMetadata.getAvailability() != null)
				&& (rMetadata.getAvailability().equals(ResourceMetadata.STATUS_TYPE.OFFLINE.toString()))) {
			throw new DataInspectException("The service " + serviceId + " is " + ResourceMetadata.STATUS_TYPE.OFFLINE.toString());

		}
	}

	private void checkThreadInterrupted() throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
	}

	private void sendJobStatusInfo(final Service service, final String jobId) throws JsonProcessingException {
		StatusUpdate su = new StatusUpdate();
		su.setJobId(jobId);
		if ((service.getIsTaskManaged() != null) && (service.getIsTaskManaged().booleanValue())) {
			su.setStatus(StatusUpdate.STATUS_PENDING);
		} else {
			su.setStatus(StatusUpdate.STATUS_RUNNING);
		}
		rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
				objectMapper.writeValueAsString(su));
	}

	private boolean isAsynOrTaskManagedService(final Service service, final WorkerCallback callback, final String consumerRecordKey,
			final ExecuteServiceJob jobItem) throws InterruptedException {
		if ((service.getIsAsynchronous() != null) && (service.getIsAsynchronous().booleanValue())) {
			// Perform Asynchronous Logic
			asynchronousServiceWorker.executeService(jobItem);
			// Return null. This future will not be tracked by the Service Thread Manager.
			// TODO: Once we can simplify/isolate some of the logic, I'd like to get to a spot where
			// we don't have to scatter return statements throughout this method.
			callback.onComplete(consumerRecordKey);
			return true;
		} else if ((service.getIsTaskManaged() != null) && (service.getIsTaskManaged().booleanValue())) {
			// If this is a Task Managed service, then insert this Job into the Task Management queue.
			serviceTaskManager.addJobToQueue(jobItem);
			callback.onComplete(consumerRecordKey);
			return true;
		}

		return false;
	}

	private void interruptJob(final String jobId, final String exception) {
	logger.log(String.format("Thread for Job %s was interrupted.", jobId), Severity.INFORMATIONAL, new AuditElement("serviceController", "cancelledServiceJob", jobId));
		StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_CANCELLED);
		TextResult result = new TextResult(exception);
		statusUpdate.setResult(result);
		statusUpdate.setJobId(jobId);
		try {
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
					objectMapper.writeValueAsString(statusUpdate));
		} catch (JsonProcessingException jsonException) {
			LOG.error(JSON_ERR, jsonException);
			logger.log(String.format(
					"Error sending Cancelled Status from Job %s: %s. The Job was cancelled, but its status will not be updated in the Job Manager.",
					jobId, jsonException.getMessage()), Severity.ERROR, new AuditElement("serviceController", "errorCancellingServiceJob", jobId));
		}
	}

	private void checkServiceResponseCode(final ResponseEntity<String> externalServiceResponse) throws PiazzaJobException {
		if (externalServiceResponse.getStatusCode().is2xxSuccessful() == false) {
			throw new PiazzaJobException(String.format("Error %s with Status Code %s", externalServiceResponse.getBody(),
					externalServiceResponse.getStatusCode().toString()), externalServiceResponse.getStatusCode().value());
		}
	}

	private void sendJobStatusResultUpdate(final DataResult result, final String jobId) throws JsonProcessingException {
		if (result != null) {
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
			statusUpdate.setResult(result);
			statusUpdate.setJobId(jobId);
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
					objectMapper.writeValueAsString(statusUpdate));
		}
	}

	private ResponseEntity<String> executeExternalService(final PiazzaJobType jobType) throws InterruptedException {
		try {
			return esHandler.handle(jobType);
		} catch (Exception exception) {
			// InterruptedException to ensure a common handled exception type.
			LOG.info("Exception occurred", exception);
			throw new InterruptedException(exception.getMessage());
		}
	}

	private AsyncResult<String> processExernalServiceExecution(final Job job, WorkerCallback callback)
			throws InterruptedException, DataInspectException {

		logger.log("ExecuteServiceJob Detected with ID " + job.getJobId(), Severity.DEBUG,
				new AuditElement(job.getJobId(), "executeService", ""));

		ResponseEntity<String> externalServiceResponse = null;
		
		try {
			// Get the Job Data and Service information.
			final PiazzaJobType jobType = job.getJobType();
			final ExecuteServiceJob jobItem = (ExecuteServiceJob) jobType;
			jobItem.setJobId(job.getJobId());
			final ExecuteServiceData esData = jobItem.data;
			final Service service = accessor.getServiceById(esData.getServiceId());

			// Send the Job Status that this Job has been handled. If this is a typical sync/async service, then the
			// request will be made directly to the Service URL. If this is a Task-Managed Service, then the Job
			// will be put into the Jobs queue.
			sendJobStatusInfo(service, job.getJobId());

			if (esData.getDataOutput() != null) {

				checkThreadInterrupted();

				// First check to see if the service is OFFLINE, if so
				// do not execute a thing
				validateResourceMetadata(service.getResourceMetadata(), esData.getServiceId());

				// Determine if this is a Service that is processed Asynchronously, or is Task Managed. If so, then
				// branch here.
				if (isAsynOrTaskManagedService(service, callback, job.getJobId(), jobItem)) {
					return null;
				}

				// If Service is neither Asynchronous nor Task Managed, process it synchronously
				logger.log("ExecuteServiceJob Original Way", Severity.DEBUG);

				// Execute the external Service and get the Response Entity
				externalServiceResponse = executeExternalService(jobType);

				checkThreadInterrupted();

				// If an internal error occurred during Service Handling, then throw an exception.
				checkServiceResponseCode(externalServiceResponse);

				// Process the Response and handle any Ingest that may result
				final String dataId = uuidFactory.getUUID();
				final String outputType = jobItem.data.getDataOutput().get(0).getClass().getSimpleName();
				final DataResult result = esHandler.processExecutionResult(service, outputType, StatusUpdate.STATUS_SUCCESS,
						externalServiceResponse, dataId);

				checkThreadInterrupted();

				// If there is a Result, Send the Status Update with Result to the Job Manager component
				sendJobStatusResultUpdate(result, job.getJobId());

				// Fire Event to Workflow
				fireWorkflowEvent(job.getCreatedBy(), job.getJobId(), dataId, "Service completed successfully.");
			} else {
				externalServiceResponse = new ResponseEntity<>(
						"DataOuptut mimeType was not specified.  Please refer to the API for details.", HttpStatus.BAD_REQUEST);
			}
		} catch (IOException | ResourceAccessException ex) {
			LOG.error("Exception occurred", ex);
			logger.log(ex.getMessage(), Severity.ERROR);
			sendErrorStatus(StatusUpdate.STATUS_ERROR, ex.getMessage(), 400, job.getJobId());
		} catch (HttpClientErrorException | HttpServerErrorException hex) {
			LOG.error("HttpException occurred", hex);
			logger.log(hex.getMessage(), Severity.ERROR);
			sendErrorStatus(StatusUpdate.STATUS_ERROR, hex.getResponseBodyAsString(), hex.getStatusCode().value(), job.getJobId());
		} catch (PiazzaJobException pex) {
			LOG.error("PiazzaJobException occurred", pex);
			logger.log(pex.getMessage(), Severity.ERROR);
			sendErrorStatus(StatusUpdate.STATUS_ERROR, pex.getMessage(), pex.getStatusCode(), job.getJobId());
		}

		checkThreadInterrupted();

		if (externalServiceResponse != null && externalServiceResponse.getStatusCode() != HttpStatus.OK) {
			sendErrorStatus(StatusUpdate.STATUS_FAIL, externalServiceResponse, new Integer(externalServiceResponse.getStatusCode().value()),
					job.getJobId());
		}

		// Return Future
		callback.onComplete(job.getJobId());

		return new AsyncResult<>("ServiceMessageWorker_Thread");
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
	private void sendErrorStatus(String status, Object message, Integer statusCode, String jobId) {
		StatusUpdate statusUpdate = new StatusUpdate();
		statusUpdate.setStatus(status);
		// Create a text result and update status
		ErrorResult errorResult = new ErrorResult();
		errorResult.setMessage(message);
		errorResult.setStatusCode(statusCode);
		statusUpdate.setResult(errorResult);
		statusUpdate.setJobId(jobId);

		try {
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
					objectMapper.writeValueAsString(statusUpdate));
		} catch (JsonProcessingException exception) {
			// The message could not be serialized. Record this.
			LOG.error(JSON_ERR, exception);
			logger.log("Could not send Error Status to Job Manager. Error serializing Status: " + exception.getMessage(), Severity.ERROR);
		}
	}

	/**
	 * Fires the event to the Workflow service that a Service has completed execution.
	 */
	private void fireWorkflowEvent(String user, String jobId, String dataId, String message) {
		logger.log("Firing Event for Completion of ExecuteServiceJob execution", Severity.DEBUG,
				new AuditElement(jobId, "executeServiceWorkflowEventCreated", dataId));
		try {
			// Retrieve piazza:executionCompletion EventTypeId from pz-workflow.
			String url = String.format("%s/%s?name=%s", WORKFLOW_URL, "eventType", "piazza:executionComplete");
			EventType eventType = objectMapper.readValue(restTemplate.getForObject(url, String.class), EventTypeListResponse.class).data
					.get(0);

			// Construct Event object
			Map<String, Object> data = new HashMap<>();
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
			String error = String.format("Could not successfully send Event to Workflow Service. Returned with code %s and message %s",
					exception.getStatusCode().toString(), exception.getResponseBodyAsString());
			logger.log(error, Severity.ERROR);
			LOG.error(error, exception);
		} catch (IOException exception) {
			String error = String.format("Could not send Event to Workflow Service. Serialization of Event failed with Error: %s",
					exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
		}
	}

	private UriComponentsBuilder processURLParameterDataTypeMetadata(final String paramValue, final String inputName,
			final Service sMetadata, final UriComponentsBuilder builder) {

		UriComponentsBuilder localBuilder = builder;

		if (inputName.length() == 0) {
			logger.log("sMetadata.getResourceMeta=" + sMetadata.getResourceMetadata(), Severity.DEBUG);
			localBuilder = UriComponentsBuilder.fromHttpUrl(sMetadata.getUrl() + "?" + paramValue);
			logger.log("Builder URL is " + localBuilder.toUriString(), Severity.DEBUG);
		} else {
			localBuilder.queryParam(inputName, paramValue);
			logger.log("Input Name=" + inputName + " paramValue=" + paramValue, Severity.DEBUG);
		}

		return localBuilder;
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
	public void handleRasterType(ExecuteServiceJob executeJob, Job job)
			throws InterruptedException, JsonParseException, JsonMappingException, IOException {

		RestTemplate restTemplate = new RestTemplate();
		ExecuteServiceData data = executeJob.data;

		// Get the id from the data
		String serviceId = data.getServiceId();
		Service sMetadata = accessor.getServiceById(serviceId);

		// Default request mimeType application/json
		String requestMimeType = "application/json";

		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getUrl());
		Map<String, DataType> postObjects = new HashMap<>();
		Iterator<Entry<String, DataType>> it = data.getDataInputs().entrySet().iterator();
		String postString = "";

		while (it.hasNext()) {
			Entry<String, DataType> entry = it.next();
			String inputName = entry.getKey();

			if (entry.getValue() instanceof URLParameterDataType) {
				String paramValue = ((TextDataType) entry.getValue()).getContent();

				builder = processURLParameterDataTypeMetadata(paramValue, inputName, sMetadata, builder);
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
				LOG.error(JSON_ERR, e);
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

			logger.log("The postString is " + postString, Severity.DEBUG);

			HttpHeaders theHeaders = new HttpHeaders();
			// headers.add("Authorization", "Basic " + credentials);
			theHeaders.setContentType(MediaType.APPLICATION_JSON);

			// Create the Request template and execute
			HttpEntity<String> request = new HttpEntity<>(postString, theHeaders);

			logger.log("About to call special service " + url, Severity.DEBUG);

			if (null != sMetadata.getTimeout()) {
				HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
				factory.setReadTimeout(sMetadata.getTimeout().intValue() * 1000);
				factory.setConnectTimeout(sMetadata.getTimeout().intValue() * 1000);
				restTemplate = new RestTemplate(factory);
			}

			ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

			checkThreadInterrupted();

			logger.log("The Response is " + response.getBody(), Severity.DEBUG);

			String serviceControlString = response.getBody();
			logger.log("Service Control String " + serviceControlString, Severity.DEBUG);

			DataResource dataResource = objectMapper.readValue(serviceControlString, DataResource.class);
			logger.log("dataResource type is " + dataResource.getDataType().getClass().getSimpleName(), Severity.DEBUG);

			dataResource.dataId = uuidFactory.getUUID();
			logger.log("dataId " + dataResource.dataId, Severity.DEBUG);

			PiazzaJobRequest pjr = new PiazzaJobRequest();
			pjr.createdBy = "pz-sc-ingest-raster-test";

			IngestJob ingestJob = new IngestJob();
			ingestJob.data = dataResource;
			ingestJob.host = true;
			pjr.jobType = ingestJob;

			// Send to Message Bus
			pjr.jobId = uuidFactory.getUUID();
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, requestJobQueue.getName(),
					objectMapper.writeValueAsString(pjr));

			logger.log(String.format("newProdRecord sent with Job ID %s", pjr.jobId), Severity.DEBUG);

			checkThreadInterrupted();

			fireWorkflowEvent(job.getCreatedBy(), job.getJobId(), dataResource.dataId, "Service completed successfully.");

			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);

			// Create a text result and update status
			DataResult textResult = new DataResult(dataResource.dataId);
			statusUpdate.setResult(textResult);
			statusUpdate.setJobId(job.getJobId());
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
					objectMapper.writeValueAsString(statusUpdate));

			logger.log("Job Update with ID sent " + job.getJobId(), Severity.DEBUG);
		}
	}

	public MediaType createMediaType(String mimeType) {
		MediaType mediaType;
		String type, subtype;
		StringBuilder sb = new StringBuilder(mimeType);
		int index = sb.indexOf("/");
		// If a slash was found then there is a type and subtype
		if (index != -1) {
			type = sb.substring(0, index);

			subtype = sb.substring(index + 1, mimeType.length());
			mediaType = new MediaType(type, subtype);
			logger.log("The type is=" + type, Severity.DEBUG);
			logger.log("The subtype is=" + subtype, Severity.DEBUG);

		} else {
			// Assume there is just a type for the mime, no subtype
			mediaType = new MediaType(mimeType);
		}

		return mediaType;
	}
}
