package org.venice.piazza.servicecontroller.messaging;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import org.venice.piazza.servicecontroller.util.CoreUUIDGen;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.DataType;
import model.data.type.BodyDataType;
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
import model.service.metadata.ParamDataItem;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import util.PiazzaLogger;
import util.UUIDFactory;

public class ServiceMessageWorker implements Runnable {
	
	private final static String TEXT_TYPE="text";
	private final static String RASTER_TYPE="raster";
	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceMessageWorker.class);
	private MongoAccessor accessor;
	private PiazzaLogger coreLogger;
	private CoreServiceProperties coreServiceProperties;	
	private Job job = null;
	private ConsumerRecord<String, String> consumerRecord;
	private Producer<String, String> producer;
	private WorkerCallback callback;
	private UUIDFactory uuidFactory;
	private CoreUUIDGen uuidGenerator;
	/**
	 * Initializes the ServiceMessageWorker which works on handling the jobRequest
	 * @param consumerRecord
	 * @param producer
	 * @param callback
	 * @param uuidFactory
	 * @param logger
	 * @param jobType
	 */
	public ServiceMessageWorker (ConsumerRecord<String, String> consumerRecord,
			Producer<String, String> producer, MongoAccessor accessor, WorkerCallback callback, 
			CoreServiceProperties coreServiceProperties, UUIDFactory uuidFactory, 
			PiazzaLogger logger,Job job) {
		this.job = job;
		this.consumerRecord = consumerRecord;
		this.producer = producer;
		this.accessor = accessor;
		this.callback = callback;
		this.coreLogger = logger;
		this.uuidFactory = uuidFactory;
		this.uuidGenerator = uuidGenerator;
		
		
	}
	
	/**
	 * Handles service job requests
	 */
	public void run() {
		try {
			String handleUpdate = StatusUpdate.STATUS_SUCCESS;
			String handleTextUpdate = "";
			ResponseEntity<List<String>> handleResult = null;
			ObjectMapper mapper = new ObjectMapper();
	
			try {
	
				// if a jobType has been declared
				if (job != null) {
	
					PiazzaJobType jobType = job.getJobType();
					LOGGER.debug("Job ID:" + job.getJobId());

					if (jobType instanceof RegisterServiceJob) {
					   LOGGER.debug("RegisterServiceJob Detected");
					   // Handle Register Job
					   RegisterServiceHandler rsHandler = new RegisterServiceHandler(accessor, coreServiceProperties, coreLogger, uuidFactory);
					   handleResult = rsHandler.handle(jobType);
					   handleResult = checkResult(handleResult);
					   sendRegisterStatus(job, handleUpdate, handleResult);
							
					} else if (jobType instanceof ExecuteServiceJob) {	
						LOGGER.debug("ExecuteServiceJob Detected");

						// Get the ResourceMetadata
						ExecuteServiceJob jobItem = (ExecuteServiceJob)jobType;
						ExecuteServiceData esData = jobItem.data;
						DataType dataType= esData.getDataOutput();
						if ((dataType != null) && (dataType instanceof RasterDataType)) {
							// Call special method to call and send
							handleRasterType(jobItem);
						}
						else {
							LOGGER.debug("ExecuteServiceJob Original Way");

							ExecuteServiceHandler esHandler = new ExecuteServiceHandler(accessor, coreServiceProperties, coreLogger);
							handleResult = esHandler.handle(jobType);
							LOGGER.debug("Execution handled");
							handleResult = checkResult(handleResult);
							LOGGER.debug("Send Execute Status KAFKA");

							sendExecuteStatus(job, handleUpdate, handleResult);
						}
					} 
					else if (jobType instanceof UpdateServiceJob) {
						UpdateServiceHandler usHandler = new UpdateServiceHandler(accessor, coreServiceProperties, coreLogger, uuidFactory);
						handleResult = usHandler.handle(jobType);
						handleResult = checkResult(handleResult);
						sendUpdateStatus(job, handleUpdate, handleResult);
						
					}
					else if (jobType instanceof DeleteServiceJob) {
						DeleteServiceHandler dlHandler = new DeleteServiceHandler(accessor, coreServiceProperties, coreLogger, uuidFactory);
					    handleResult = dlHandler.handle(jobType);	
					    handleResult = checkResult(handleResult);
						sendDeleteStatus(job, handleUpdate, handleResult);
		
					}
					else if (jobType instanceof DescribeServiceMetadataJob) {
						DescribeServiceHandler dsHandler = new DescribeServiceHandler(accessor, coreServiceProperties, coreLogger);
					    handleResult = dsHandler.handle(jobType);
					    handleResult = checkResult(handleResult);
						sendDescribeStatus(job, handleUpdate, handleResult);
					    
					}
					else if (jobType instanceof ListServicesJob) {
					   ListServiceHandler lsHandler = new ListServiceHandler(accessor, coreServiceProperties, coreLogger);  
					   handleResult = lsHandler.handle(jobType);
					   handleResult = checkResult(handleResult);
					   sendListStatus(job, handleUpdate, handleResult);
						
					}
					else if (jobType instanceof SearchServiceJob) {
					    LOGGER.debug("SearchService Job Detected");
						SearchServiceHandler ssHandler = new SearchServiceHandler(accessor, coreServiceProperties, coreLogger);
					   
						handleResult = ssHandler.handle(jobType);
						 LOGGER.debug("Performed Search Job Detected");
						handleResult = checkResult(handleResult);
					    sendSearchStatus(job, handleUpdate, handleResult);
					}
				}// if job not null
			} catch (IOException ex) {
				LOGGER.error(ex.getMessage());
				handleUpdate = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = ex.getMessage();
			}
			catch (ResourceAccessException rex) {
				LOGGER.error(rex.getMessage());
				handleTextUpdate = rex.getMessage();
				handleUpdate = StatusUpdate.STATUS_ERROR;
			}
			catch (HttpClientErrorException hex) {
				LOGGER.error(hex.getMessage());
				handleUpdate = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = hex.getMessage();
			}
			
		    // if there was no result set then 
			// use the default error messages set.
			if (handleResult == null) {
				
				StatusUpdate su = new StatusUpdate();
				su.setStatus(handleUpdate);
				// Create a text result and update status
				ErrorResult errorResult = new ErrorResult();
				errorResult.setMessage(handleTextUpdate);
				
				su.setResult(errorResult);
	
				
				ProducerRecord<String,String> prodRecord =
						new ProducerRecord<String,String> (JobMessageFactory.UPDATE_JOB_TOPIC_NAME,job.getJobId(),
								mapper.writeValueAsString(su));
				producer.send(prodRecord);
			}
			// If the status is not ok and the job is not equal to null
			// then send an update to the job manager that there was some failure
			else {
				boolean eResult = ((handleResult.getStatusCode() != HttpStatus.OK) && (job != null))?false:false;
			    if (eResult) {
					handleUpdate =  StatusUpdate.STATUS_FAIL;
				
			    
					handleResult = checkResult(handleResult);
	
					String serviceControlString = mapper.writeValueAsString(handleResult);
	
					StatusUpdate su = new StatusUpdate();
					su.setStatus(handleUpdate);
					// Create a text result and update status
					ErrorResult errorResult = new ErrorResult();
					errorResult.setMessage(serviceControlString);
					su.setResult(errorResult);
	
					
					ProducerRecord<String,String> prodRecord =
							new ProducerRecord<String,String> (JobMessageFactory.UPDATE_JOB_TOPIC_NAME,job.getJobId(),
									mapper.writeValueAsString(su));
					producer.send(prodRecord);
			    }
	
				
			}
		} catch (WakeupException ex) {
			LOGGER.error(ex.getMessage());
		} catch (JsonProcessingException ex) {
			LOGGER.error(ex.getMessage());
		}
		
	}



	
	private void sendListStatus(Job job, String status, ResponseEntity<List<String>> handleResult)  throws JsonProcessingException {
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			
			su.setStatus(StatusUpdate.STATUS_SUCCESS);
			List <String>stringList = handleResult.getBody();
			
			TextResult textResult = new TextResult();
				textResult.setText(stringList.get(0));
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {
				
				LOGGER.debug("THe STATUS is " + su.getStatus());
				LOGGER.debug("THe RESULT is " + su.getResult());
	
				ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su);
				
				producer.send(prodRecord);
			}
			else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(stringList.get(0), handleResult.getStatusCode().toString()));
	            producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su));
			}
		}
	}
	/** 
	 * Sends an update for registering a job
	 * 
	 */
	private void sendRegisterStatus(Job job, String status, ResponseEntity<List<String>> handleResult)  throws JsonProcessingException {
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			
			su.setStatus(StatusUpdate.STATUS_SUCCESS);
			List <String>stringList = handleResult.getBody();
			
			TextResult textResult = new TextResult();
				textResult.setText(stringList.get(0));
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {
				
				LOGGER.debug("THe STATUS is " + su.getStatus());
				LOGGER.debug("THe RESULT is " + su.getResult());
	
				ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su);
				
				producer.send(prodRecord);
			}
			else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(stringList.get(0), handleResult.getStatusCode().toString()));
	            producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su));
			}
		}
	}
	
	/**
	 * Sends the list of services to the job
	 */
	
	private void sendSearchStatus(Job job, String status, ResponseEntity<List<String>> handleResult)  throws JsonProcessingException {
		LOGGER.info("sendSearchStatus to jobmanager");
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			
			su.setStatus(StatusUpdate.STATUS_SUCCESS);
			List <String>stringList = handleResult.getBody();
			
			TextResult textResult = new TextResult();
				textResult.setText(stringList.get(0));
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {
				
				LOGGER.info("THe STATUS is " + su.getStatus());
				LOGGER.info("THe RESULT is " + su.getResult());
	
				ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su);
				
				producer.send(prodRecord);
			}
			else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(stringList.get(0), "No Results returned from the search. HTTP Status:" + handleResult.getStatusCode().toString()));
	            producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su));
			}
		}else {
			LOGGER.info("There are no search results that match");
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			
			su.setStatus(StatusUpdate.STATUS_SUCCESS);
			
			
			TextResult textResult = new TextResult();
				textResult.setText("");
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {
				
				LOGGER.info("THe STATUS is " + su.getStatus());
				LOGGER.info("THe RESULT is " + su.getResult());
	
				ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su);
				
				producer.send(prodRecord);
			}
		}
	}
	
	/** 
	 * Sends an update for registering a job
	 * Message is sent on Kafka Queue
	 * 
	 */
	private void sendUpdateStatus(Job job, String status, ResponseEntity<List<String>> handleResult)  throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		String serviceControlString = mapper.writeValueAsString(handleResult.getBody());
		StatusUpdate su = new StatusUpdate();
		su.setStatus(serviceControlString);
		ProducerRecord<String,String> prodRecord =
				new ProducerRecord<String,String> (JobMessageFactory.UPDATE_JOB_TOPIC_NAME,job.getJobId(),
						mapper.writeValueAsString(su));
		producer.send(prodRecord);
	}
	
	/** 
	 * Sends an update for deleting the resource
	 * Resource is not deleted but marked as unavailable
	 * Message is sent on Kafka Queue
	 * 
	 */
	private void sendDeleteStatus(Job job, String status, ResponseEntity<List<String>> handleResult)  throws JsonProcessingException {	
		
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			
			su.setStatus(StatusUpdate.STATUS_SUCCESS);
			List <String>stringList = handleResult.getBody();
			TextResult textResult = new TextResult();
			// Get the resource ID and set it as the result
			textResult.setText(stringList.get(1));
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {
				
				LOGGER.debug("THe STATUS is " + su.getStatus());
				LOGGER.debug("THe RESULT is " + su.getResult());
	
				ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su);
				
				producer.send(prodRecord);
			}
		
			else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(stringList.get(0), "Resource cold not be deleted. HTTP Status:" + handleResult.getStatusCode().toString()));
	            producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su));
			}
		}
		
	}
	
	/** 
	 * Sends an update for describing the resource
	 * Message is sent on Kafka Queue
	 * 
	 */
	private void sendDescribeStatus(Job job, String status, ResponseEntity<List<String>> handleResult)  throws JsonProcessingException {	
		
		if (handleResult != null) {
			// Create a text result and update status
			StatusUpdate su = new StatusUpdate();
			
			su.setStatus(StatusUpdate.STATUS_SUCCESS);
			List <String>stringList = handleResult.getBody();
			
			TextResult textResult = new TextResult();
				textResult.setText(stringList.get(0));
			su.setResult(textResult);
			if (handleResult.getStatusCode() == HttpStatus.OK) {
				
				LOGGER.debug("THe STATUS is " + su.getStatus());
				LOGGER.debug("THe RESULT is " + su.getResult());
	
				ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su);
				
				producer.send(prodRecord);
			}
		
			else {
				su = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				su.setResult(new ErrorResult(stringList.get(0), "Resource cold not be deleted. HTTP Status:" + handleResult.getStatusCode().toString()));
	            producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su));
			}
		}
		
	}
	
	/**
	 * Send an execute job status and the resource that was used
	 * Message is sent on Kafka Queue
	 * @param job
	 * @param status
	 * @param handleResult
	 * @throws JsonProcessingException
	 */
	private void sendExecuteStatus(Job job, String status, ResponseEntity<List<String>> handleResult)  throws JsonProcessingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        
        LOGGER.debug("The result provided from service is " + handleResult.getBody());
        LOGGER.debug("==============================================================");
        String serviceControlString = mapper.writeValueAsString(handleResult.getBody());
        LOGGER.debug("The service controller string is " + serviceControlString);
        // Now produce a new record
        PiazzaJobRequest pjr  =  new PiazzaJobRequest();
        // TODO read from properties file
        pjr.userName = "pz-sc-ingest";
        IngestJob ingestJob = new IngestJob();
        DataResource data = new DataResource();
        data.dataId = uuidFactory.getUUID();

        ObjectMapper tempMapper = new ObjectMapper(); 
        try {
        	
        	 data = tempMapper.readValue(serviceControlString, DataResource.class);
        
        } catch (JsonProcessingException jpe) {
			jpe.printStackTrace();
	        TextDataType tr = new TextDataType();
	        tr.content = serviceControlString;
	        data.dataType = tr;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
        
        //LOGGER.info("The content of the result is " + serviceControlString);
        
        ingestJob.data=data;
        ingestJob.host = true;

        pjr.jobType  = ingestJob;

        // TODO Generate 123-456 with UUIDGen
        String jobId = uuidFactory.getUUID();
        ProducerRecord<String,String> newProdRecord =
        JobMessageFactory.getRequestJobMessage(pjr, jobId);
      
        producer.send(newProdRecord);
        coreLogger.log(String.format("Sending Ingest Job ID %s for Data ID %s for Data of Type %s", jobId, data.getDataId(), data.getDataType().getType()), PiazzaLogger.INFO);

        StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);

        // Create a text result and update status
        DataResult textResult = new DataResult(data.dataId);

        statusUpdate.setResult(textResult);


        ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate);

        producer.send(prodRecord);
	}
	
	/**
	 *  This method is for demonstrating ingest of raster data
	 *  This will be refactored once the API changes have been communicated to
	 *  other team members
	 */
	public void handleRasterType(ExecuteServiceJob executeJob) {
		
		RestTemplate restTemplate = new RestTemplate();
		ExecuteServiceData data = executeJob.data;
		// Get the id from the data
			String serviceId = data.getServiceId();
			Service sMetadata = accessor.getServiceById(serviceId);
			// Default request mimeType application/json
			String requestMimeType = "application/json";
			MultiValueMap<String, String> map = new LinkedMultiValueMap<String, String>();
		
			UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getResourceMetadata().url);
			Set<String> parameterNames = new HashSet<String>();
			if (sMetadata.getInputs() != null && sMetadata.getInputs().size() > 0) {
				for (ParamDataItem pdataItem : sMetadata.getInputs()) {
					if (pdataItem.getDataType() instanceof URLParameterDataType) {
						parameterNames.add(pdataItem.getName());
					}
				}
			}
			Map<String,DataType> postObjects = new HashMap<String,DataType>();
			Iterator<Entry<String,DataType>> it = data.getDataInputs().entrySet().iterator();
			String postString = "";
			while (it.hasNext()) {
				Entry<String,DataType> entry = it.next();
				
				String inputName = entry.getKey();
				if (parameterNames.contains(inputName)) {
					if (entry.getValue() instanceof TextDataType) {
						String paramValue = ((TextDataType)entry.getValue()).getContent();
						if (inputName.length() == 0) {
							builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getResourceMetadata().url + "?" + paramValue);
						}
						else {
							 builder.queryParam(inputName,paramValue);
						}
					}
					else {
						LOGGER.error("URL parameter value has to be specified in TextDataType" );
						return;
					}
				}
				else if (entry.getValue() instanceof BodyDataType){
					BodyDataType bdt = (BodyDataType)entry.getValue();
					postString = bdt.getContent();
					requestMimeType = bdt.getMimeType();
				}
				//Default behavior for other inputs, put them in list of objects
				// which are transformed into JSON consistent with default requestMimeType
				else {
					postObjects.put(inputName, entry.getValue());
				}
			}
			if (postString.length() > 0 && postObjects.size() > 0) {
				LOGGER.error("String Input not consistent with other Inputs");
				return;
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
			HttpHeaders headers = new HttpHeaders();
			
			// Set the mimeType of the request
			MediaType mediaType = createMediaType(requestMimeType);
			headers.setContentType(mediaType);
			// Set the mimeType of the request
			//headers.add("Content-type", sMetadata.getOutputs().get(0).getDataType().getMimeType());
			HttpEntity<String> requestEntity = null;
			if (postString.length() > 0) {
				LOGGER.debug("The postString is " + postString);
				//requestEntity = this.buildHttpEntity(sMetadata, headers, postString);
				HttpHeaders theHeaders = new HttpHeaders();
				//headers.add("Authorization", "Basic " + credentials);
				theHeaders.setContentType(MediaType.APPLICATION_JSON);

				// Create the Request template and execute
				HttpEntity<String> request = new HttpEntity<String>(postString, theHeaders);
				
		
			
				try {  
					
				    LOGGER.debug("About to call special service");
				    LOGGER.debug("URL calling" + url);
                    // COMMENT OUT CALLING SERVICE
				    ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);
				    LOGGER.debug("The Response is " + response.getBody());
				    // DOES NOT RETURN!!!!!!
		            //DataResource dataResource = response.getBody();
				    
				    ObjectMapper mapper = new ObjectMapper();

				    String serviceControlString = response.getBody();
			        LOGGER.debug("Service Control String" + serviceControlString); 

			        ObjectMapper tempMapper = new ObjectMapper();
				    DataResource dataResource = tempMapper.readValue(serviceControlString, DataResource.class);
			        
			        LOGGER.debug("This is a test");
			        LOGGER.debug("dataResource type is" + dataResource.getDataType().getType());

				    //DataResource dataResource = new DataResource();
		            dataResource.dataId = uuidFactory.getUUID();
		            LOGGER.debug("dataId" + dataResource.dataId);
		            PiazzaJobRequest pjr  =  new PiazzaJobRequest();
		            pjr.userName = "pz-sc-ingest-raster-test";
		            
		            IngestJob ingestJob = new IngestJob();
		            ingestJob.data=dataResource;
		            ingestJob.host = true;
		            pjr.jobType  = ingestJob;
		            ProducerRecord<String,String> newProdRecord =
		            JobMessageFactory.getRequestJobMessage(pjr, uuidFactory.getUUID());
		
		             producer.send(newProdRecord);
		             LOGGER.debug("newProdRecord sent" + newProdRecord.toString());
		             StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
		
		             // Create a text result and update status
		             DataResult textResult = new DataResult(dataResource.dataId);
		
		             statusUpdate.setResult(textResult);
		
		             ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate);
		
		             producer.send(prodRecord);
		             LOGGER.debug("prodRecord sent" + prodRecord.toString());
	
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
		HttpEntity<String> requestEntity = new HttpEntity<String>(data,headers);
		return requestEntity;
	
	}
	
    /**
     * Check to see if there is a valid handleResult that was created.  If not,
     * then create a message with No Content
     * @param handleResult
     * @return handleResult - Created if the result is not valid
     */
	private ResponseEntity<List<String>> checkResult(ResponseEntity<List<String>> handleResult) {
		if (handleResult == null) {
			handleResult = new ResponseEntity<List<String>>(new ArrayList<String>(),HttpStatus.NO_CONTENT);
			
		}
		
		return handleResult;
	}

}
