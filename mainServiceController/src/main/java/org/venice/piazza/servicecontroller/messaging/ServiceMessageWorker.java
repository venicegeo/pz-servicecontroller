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
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.DeleteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.DescribeServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ListServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.SearchServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.UpdateServiceHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import model.job.result.type.TextResult;
import model.job.type.DeleteServiceJob;
import model.job.type.DescribeServiceMetadataJob;
import model.job.type.ExecuteServiceJob;
import model.job.type.IngestJob;
import model.job.type.ListServicesJob;
import model.job.type.RegisterServiceJob;
import model.job.type.SearchServiceJob;
import model.job.type.UpdateServiceJob;
import model.request.PiazzaJobRequest;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import model.status.StatusUpdate;
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

	@Autowired
	private UUIDFactory uuidFactory;
	
	@Autowired
	private MongoAccessor accessor;
	
	@Autowired
	private PiazzaLogger coreLogger;
	
	@Autowired
	private DeleteServiceHandler dlHandler;

	@Autowired
	private DescribeServiceHandler dsHandler;

	@Autowired
	private ExecuteServiceHandler esHandler;

	@Autowired
	private ListServiceHandler lsHandler;

	@Autowired
	private RegisterServiceHandler rsHandler;

	@Autowired
	private SearchServiceHandler ssHandler;

	@Autowired
	private UpdateServiceHandler usHandler;
	
	/**
	 * Handles service job requests on a thread
	 */
	@Async
	public Future<String> run(ConsumerRecord<String, String> consumerRecord, Producer<String, String> producer, Job job, WorkerCallback callback){
		try {
			String handleUpdate = StatusUpdate.STATUS_SUCCESS;
			String handleTextUpdate = "";
			ResponseEntity<String> handleResult = null;
			boolean rasterJob = false;
			ObjectMapper mapper = new ObjectMapper();

			try {
				// if a jobType has been declared
				if (job != null) {

					PiazzaJobType jobType = job.getJobType();
					coreLogger.log("Job ID:" + job.getJobId(), PiazzaLogger.DEBUG);

					if (jobType instanceof RegisterServiceJob) {
						coreLogger.log("RegisterServiceJob Detected", PiazzaLogger.DEBUG);

						// Handle Register Job
						handleResult = rsHandler.handle(jobType);
						handleResult = checkResult(handleResult);
						sendRegisterStatus(job, producer, handleUpdate, handleResult);
					} else if (jobType instanceof ExecuteServiceJob) {
						coreLogger.log("ExecuteServiceJob Detected", PiazzaLogger.DEBUG);

						// Get the ResourceMetadata
						ExecuteServiceJob jobItem = (ExecuteServiceJob) jobType;
						ExecuteServiceData esData = jobItem.data;
						DataType dataType = esData.dataOutput.get(0);
						if ((dataType != null) && (dataType instanceof RasterDataType)) {
							// Call special method to call and send
							rasterJob = true;
							handleRasterType(jobItem, job, producer);

							sendExecuteStatus(job, producer, handleUpdate, handleResult);
						} else {
							coreLogger.log("ExecuteServiceJob Original Way", PiazzaLogger.DEBUG);
							handleResult = esHandler.handle(jobType);

							coreLogger.log("Execution handled", PiazzaLogger.DEBUG);
							handleResult = checkResult(handleResult);

							coreLogger.log("Send Execute Status KAFKA", PiazzaLogger.DEBUG);
							sendExecuteStatus(job, producer, handleUpdate, handleResult);
						}
					} else if (jobType instanceof UpdateServiceJob) {
						coreLogger.log("UpdateServiceJob", PiazzaLogger.DEBUG);

						handleResult = usHandler.handle(jobType);
						handleResult = checkResult(handleResult);
						sendUpdateStatus(job, producer, handleUpdate, handleResult);

					} else if (jobType instanceof DeleteServiceJob) {
						coreLogger.log("DeleteServiceJob", PiazzaLogger.DEBUG);

						handleResult = dlHandler.handle(jobType);
						handleResult = checkResult(handleResult);
						sendDeleteStatus(job, producer, handleUpdate, handleResult);

					} else if (jobType instanceof DescribeServiceMetadataJob) {
						handleResult = dsHandler.handle(jobType);
						handleResult = checkResult(handleResult);
						sendDescribeStatus(job, producer, handleUpdate, handleResult);

					} else if (jobType instanceof ListServicesJob) {
						handleResult = lsHandler.handle(jobType);
						handleResult = checkResult(handleResult);
						sendListStatus(job, producer, handleUpdate, handleResult);

					} else if (jobType instanceof SearchServiceJob) {
						handleResult = ssHandler.handle(jobType);

						handleResult = checkResult(handleResult);
						sendSearchStatus(job, producer, handleUpdate, handleResult);
					}
				}
			} catch (IOException ex) {
				coreLogger.log(ex.getMessage(), PiazzaLogger.ERROR);
				handleUpdate = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = ex.getMessage();
			} catch (ResourceAccessException rex) {
				coreLogger.log(rex.getMessage(), PiazzaLogger.ERROR);
				handleTextUpdate = rex.getMessage();
				handleUpdate = StatusUpdate.STATUS_ERROR;
			} catch (HttpClientErrorException hex) {
				coreLogger.log(hex.getMessage(), PiazzaLogger.ERROR);
				handleUpdate = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = hex.getMessage();
			}

			// if there was no result set then
			// use the default error messages set.
			if (!rasterJob) {
				if (handleResult == null) {

					StatusUpdate su = new StatusUpdate();
					su.setStatus(handleUpdate);
					// Create a text result and update status
					ErrorResult errorResult = new ErrorResult();
					errorResult.setMessage(handleTextUpdate);

					su.setResult(errorResult);

					ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(
							String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), job.getJobId(),
							mapper.writeValueAsString(su));
					producer.send(prodRecord);
				} else {
					// If the status is not ok and the job is not equal to null
					// then send an update to the job manager that there was some failure
					boolean eResult = ((handleResult.getStatusCode() != HttpStatus.OK) && (job != null)) ? false : false;
					if (eResult) {
						handleUpdate = StatusUpdate.STATUS_FAIL;

						handleResult = checkResult(handleResult);

						String serviceControlString = mapper.writeValueAsString(handleResult);

						StatusUpdate su = new StatusUpdate();
						su.setStatus(handleUpdate);
						// Create a text result and update status
						ErrorResult errorResult = new ErrorResult();
						errorResult.setMessage(serviceControlString);
						su.setResult(errorResult);

						ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(
								String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), job.getJobId(), mapper.writeValueAsString(su));
						producer.send(prodRecord);
					}

				}
			}
		} catch (WakeupException ex) {
			coreLogger.log(ex.getMessage(), PiazzaLogger.ERROR);
		} catch (JsonProcessingException ex) {
			coreLogger.log(ex.getMessage(), PiazzaLogger.ERROR);
		}

		return new AsyncResult<String>("ServiceMessageWorker_Thread");
	}

	/**
	 * Kafka message sending status update
	 * 
	 * @param job
	 * @param status
	 * @param handleResult
	 * @throws JsonProcessingException
	 */
	private void sendListStatus(Job job, Producer<String, String> producer, String status, ResponseEntity<String> handleResult) throws JsonProcessingException {
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			su.setStatus(StatusUpdate.STATUS_SUCCESS);

			TextResult textResult = new TextResult();
			textResult.setText(handleResult.getBody());
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {
                coreLogger.log("The STATUS is " + su.getStatus(), PiazzaLogger.DEBUG);
                coreLogger.log("The RESULT is " + su.getStatus(), PiazzaLogger.DEBUG);
				ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE);

				producer.send(prodRecord);
			} else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(handleResult.getBody(), handleResult.getStatusCode().toString()));
				producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE));
			}
		}
	}
	
	/** 
	 * Sends an update for registering a job
	 * 
	 */
	private void sendRegisterStatus(Job job, Producer<String, String> producer, String status, ResponseEntity<String> handleResult)  throws JsonProcessingException {
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			
			su.setStatus(StatusUpdate.STATUS_SUCCESS);
			
			TextResult textResult = new TextResult();
			textResult.setText(handleResult.getBody());
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {

				coreLogger.log("The STATUS is " + su.getStatus(), PiazzaLogger.DEBUG);
                coreLogger.log("The RESULT is " + su.getStatus(), PiazzaLogger.DEBUG);
				ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE);
				
				producer.send(prodRecord);
			}
			else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(handleResult.getBody(), handleResult.getStatusCode().toString()));
	            producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE));
			}
		}
	}
	
	/**
	 * Kafka message sending results of the search.
	 * 
	 * @param job
	 * @param status
	 * @param handleResult
	 * @throws JsonProcessingException
	 */
	private void sendSearchStatus(Job job, Producer<String, String> producer, String status, ResponseEntity<String> handleResult) throws JsonProcessingException {
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			su.setStatus(StatusUpdate.STATUS_SUCCESS);

			TextResult textResult = new TextResult();
			textResult.setText(handleResult.getBody());
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {

				ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE);

				producer.send(prodRecord);
			} else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(handleResult.getBody(), "No Results returned from the search. HTTP Status:" + handleResult.getStatusCode().toString()));
				
				producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE));
			}
		} else {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			su.setStatus(StatusUpdate.STATUS_SUCCESS);

			TextResult textResult = new TextResult();
			textResult.setText("");
			su.setResult(textResult);
			
			if (handleResult.getStatusCode() == HttpStatus.OK) {
				coreLogger.log("The STATUS is " + su.getStatus(), PiazzaLogger.DEBUG);
                coreLogger.log("The RESULT is " + su.getStatus(), PiazzaLogger.DEBUG);
				ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE);
				producer.send(prodRecord);
			}
		}
	}
	
	/** 
	 * Sends an update for registering a job
	 * Message is sent on Kafka Queue
	 * 
	 */
	private void sendUpdateStatus(Job job, Producer<String, String> producer, String status, ResponseEntity<String> handleResult) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		String serviceControlString = mapper.writeValueAsString(handleResult.getBody());
		StatusUpdate su = new StatusUpdate();
		su.setStatus(serviceControlString);
		ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(
				String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), job.getJobId(), mapper.writeValueAsString(su));
		producer.send(prodRecord);
	}
	
	/** 
	 * Sends an update for deleting the resource
	 * Resource is not deleted but marked as unavailable
	 * Message is sent on Kafka Queue
	 * 
	 */
	private void sendDeleteStatus(Job job, Producer<String, String> producer, String status, ResponseEntity<String> handleResult)  throws JsonProcessingException {	
		
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			su.setStatus(StatusUpdate.STATUS_SUCCESS);

			// Get the resource ID and set it as the result
			TextResult textResult = new TextResult();
			textResult.setText(handleResult.getBody());
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {
				coreLogger.log("The STATUS is " + su.getStatus(), PiazzaLogger.DEBUG);
                coreLogger.log("The RESULT is " + su.getStatus(), PiazzaLogger.DEBUG);
				ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE);
				
				producer.send(prodRecord);
			}
		
			else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(handleResult.getBody(), "Resource cold not be deleted. HTTP Status:" + handleResult.getStatusCode().toString()));
	            producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE));
			}
		}
		
	}
	
	/**
	 * Sends an update for describing the resource Message is sent on Kafka
	 * Queue
	 * 
	 */
	private void sendDescribeStatus(Job job, Producer<String, String> producer, String status, ResponseEntity<String> handleResult) throws JsonProcessingException {
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			su.setStatus(StatusUpdate.STATUS_SUCCESS);
			TextResult textResult = new TextResult();
			textResult.setText(handleResult.getBody());
			su.setResult(textResult);

			if (handleResult.getStatusCode() == HttpStatus.OK) {
				
				ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE);
				producer.send(prodRecord);
			} else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(handleResult.getBody(),
						"Resource cold not be deleted. HTTP Status:" + handleResult.getStatusCode().toString()));
				producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su, SPACE));
			}
		}
	}

	/**
	 * Send an execute job status and the resource that was used Message is sent
	 * on Kafka Queue
	 * 
	 * @param job
	 * @param status
	 * @param handleResult
	 * @throws JsonProcessingException
	 */
	private void sendExecuteStatus(Job job, Producer<String, String> producer, String status, ResponseEntity<String> handleResult)
			throws JsonProcessingException, IOException {
		coreLogger.log("The result provided from service is " + handleResult.getBody(), PiazzaLogger.DEBUG);

		//String serviceControlString = handleResult.getBody().get(0).toString();
		String serviceControlString = handleResult.getBody().toString();

		PiazzaJobType jobType = job.getJobType();
		ExecuteServiceJob jobItem = (ExecuteServiceJob) jobType;
		String type = jobItem.data.dataOutput.get(0).getClass().getSimpleName();
		coreLogger.log("The service controller string is " + serviceControlString, PiazzaLogger.DEBUG);


		// Initialize ingest job items
		DataResource data = new DataResource();
		PiazzaJobRequest pjr = new PiazzaJobRequest();

		IngestJob ingestJob = new IngestJob();
		

		try {
			// Now produce a new record
			pjr.userName = "pz-sc-ingest";
			data.dataId = uuidFactory.getUUID();
			coreLogger.log("dataId is " + data.dataId, PiazzaLogger.DEBUG);

			ObjectMapper tempMapper = new ObjectMapper();
			data = tempMapper.readValue(serviceControlString, DataResource.class);

			// Now check to see if the conversion is actually a proper DataResource
			// if it is not time to create a TextDataType and return
			if ((data == null) || (data.getDataType() == null)) {
				coreLogger.log("The DataResource is not in a valid format, creating a new DataResource and TextDataType", PiazzaLogger.DEBUG);

				data = new DataResource();
				data.dataId = uuidFactory.getUUID();
				TextDataType tr = new TextDataType();
				tr.content = serviceControlString;
				coreLogger.log("The data being sent is " + tr.content, PiazzaLogger.DEBUG);

				data.dataType = tr;
			}

		} catch (Exception ex) {
			ex.printStackTrace();

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

		ingestJob.data = data;
		ingestJob.host = true;
		pjr.jobType = ingestJob;

		// Generate 123-456 with UUIDGen
		String jobId = uuidFactory.getUUID();
		ProducerRecord<String, String> newProdRecord = JobMessageFactory.getRequestJobMessage(pjr, jobId, SPACE);
		producer.send(newProdRecord);
		
		coreLogger.log(String.format("Sending Ingest Job ID %s for Data ID %s for Data of Type %s", jobId, data.getDataId(),
				data.getDataType().getClass().getSimpleName()), PiazzaLogger.INFO);

		StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);

		// Create a text result and update status via kafka
		DataResult textResult = new DataResult(data.dataId);
		statusUpdate.setResult(textResult);
		ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate, SPACE);
		producer.send(prodRecord);
	}
	
	/**
	 * This method is for demonstrating ingest of raster data This will be
	 * refactored once the API changes have been communicated to other team
	 * members
	 */
	public void handleRasterType(ExecuteServiceJob executeJob, Job job, Producer<String, String> producer) {
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
			}
			else if (entry.getValue() instanceof BodyDataType) {
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
			ObjectMapper mapper = new ObjectMapper();
			try {
				postString = mapper.writeValueAsString(postObjects);
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

			try {
				coreLogger.log("About to call special service " + url, PiazzaLogger.DEBUG);
				

				ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);
				coreLogger.log("The Response is " + response.getBody(), PiazzaLogger.DEBUG);


				String serviceControlString = response.getBody();
				coreLogger.log("Service Control String " + serviceControlString, PiazzaLogger.DEBUG);

				ObjectMapper tempMapper = new ObjectMapper();
				DataResource dataResource = tempMapper.readValue(serviceControlString, DataResource.class);
				coreLogger.log("dataResource type is " + dataResource.getDataType().getClass().getSimpleName(), PiazzaLogger.DEBUG);

				dataResource.dataId = uuidFactory.getUUID();
				coreLogger.log("dataId " + dataResource.dataId, PiazzaLogger.DEBUG);

				PiazzaJobRequest pjr = new PiazzaJobRequest();
				pjr.userName = "pz-sc-ingest-raster-test";

				IngestJob ingestJob = new IngestJob();
				ingestJob.data = dataResource;
				ingestJob.host = true;
				pjr.jobType = ingestJob;

				ProducerRecord<String, String> newProdRecord = JobMessageFactory.getRequestJobMessage(pjr, uuidFactory.getUUID(), SPACE);
				producer.send(newProdRecord);

				coreLogger.log("newProdRecord sent " + newProdRecord.toString(), PiazzaLogger.DEBUG);

				StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);

				// Create a text result and update status
				DataResult textResult = new DataResult(dataResource.dataId);
				statusUpdate.setResult(textResult);
				ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate, SPACE);

				producer.send(prodRecord);
				coreLogger.log("prodRecord sent " + prodRecord.toString(), PiazzaLogger.DEBUG);

			} catch (JsonProcessingException jpe) {
				jpe.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
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
	
	public HttpEntity<String> buildHttpEntity(Service sMetadata, MultiValueMap<String, String> headers, String data) {
		HttpEntity<String> requestEntity = new HttpEntity<String>(data,headers);
		return requestEntity;
	}
	
    /**
     * Check to see if there is a valid handleResult that was created.  If not,
     * then create a message with No Content
     * @param handleResult
     * @return handleResult - Created if the result is not valid
     */
	private ResponseEntity<String> checkResult(ResponseEntity<String> handleResult) {
		if (handleResult == null) {
			handleResult = new ResponseEntity<String>("",HttpStatus.NO_CONTENT);
		}

		return handleResult;
	}
}
