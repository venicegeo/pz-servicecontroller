package org.venice.piazza.servicecontroller.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.DataType;
import model.data.type.RasterDataType;
import model.data.type.TextDataType;
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
			CoreUUIDGen uuidGenerator,
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
					   // Handle Register Job
					   RegisterServiceHandler rsHandler = new RegisterServiceHandler(accessor, coreServiceProperties, coreLogger, uuidFactory);
					   handleResult = rsHandler.handle(jobType);
					   handleResult = checkResult(handleResult);
					   sendRegisterStatus(job, handleUpdate, handleResult);
							
					} else if (jobType instanceof ExecuteServiceJob) {
						ExecuteServiceHandler esHandler = new ExecuteServiceHandler(accessor, coreServiceProperties, coreLogger);
						handleResult = esHandler.handle(jobType);
						handleResult = checkResult(handleResult);
						sendExecuteStatus(job, handleUpdate, handleResult);
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
						SearchServiceHandler ssHandler = new SearchServiceHandler(accessor, coreServiceProperties, coreLogger);
						handleResult = ssHandler.handle(jobType);
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
				su.setResult(new ErrorResult(stringList.get(0), "No Results returned from the search. HTTP Status:" + handleResult.getStatusCode().toString()));
	            producer.send(JobMessageFactory.getUpdateStatusMessage(job.getJobId(), su));
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
		DataResource dataResource;
		ObjectMapper mapper = new ObjectMapper();
		LOGGER.debug("The body is " + handleResult.getBody());
		String serviceControlString = mapper.writeValueAsString(handleResult.getBody().get(0));
		LOGGER.debug("string returned" + serviceControlString);
		// Now produce a new record
		PiazzaJobRequest pjr  =  new PiazzaJobRequest();		
		// TODO read from properties file
		pjr.apiKey = "pz-sc-ingest-test";
		
		// Create an ingest object
		IngestJob ingestJob = new IngestJob();
		
		// Get the JobTYpe
		ExecuteServiceJob esj = (ExecuteServiceJob)job.getJobType();
		// Now get the expected output type
        DataType outputDataType = esj.data.getDataOutput();
        
		// Get the metadata about the service for later use
		String serviceId = esj.data.getServiceId();
		Service service = accessor.getServiceById(serviceId);
		// If the type is text then create a new TextDataType
		try {
			//dataResource = mapper.readValue(serviceControlString, DataResource.class);
			ObjectMapper drMapper = new ObjectMapper();
			
			JsonNode root = drMapper.readTree(serviceControlString);
			JsonNode dataTypeNode = root.get("dataType");
			if (dataTypeNode != null)
				LOGGER.debug("Found dataType!!!!");
			LOGGER.debug("The Text of teh root is " + root.asText());

			dataResource = new DataResource();
			// Generate a unique identifier
			//dataResource.dataId = uuidFactory.getUUID();
			dataResource.dataId =  uuidGenerator.getUUID();
			DataType drDataType = dataResource.getDataType();
			
			// Check to see if the provide response matches the execute 
			// expected response
			if (drDataType.getType().equals(outputDataType.getType())) {
				// May combine all these two the same to handle
				// all of them but for now, breaking down individually
				
				if (outputDataType.getType().equals(TEXT_TYPE)) {
				
					//data.dataType = drDataType;
					ingestJob.data=dataResource;
					dataResource.metadata.name = service.getResourceMetadata().name;
					dataResource.metadata.description = service.getResourceMetadata().description;
					dataResource.metadata.url = service.getResourceMetadata().url;
					dataResource.metadata.classType = service.getResourceMetadata().classType;
					

					
				}
				// Check to see if the type is a RasterDataType
				else if (outputDataType.getType().equals(RASTER_TYPE)) {
					
					//data.dataType = drDataType;
					ingestJob.data=dataResource;
				}
			} else {
				// Log that the returned result from the service
				// does not match what the execute request requested
				coreLogger.log("Expected execution output=" + outputDataType.getType() + " Actual execution output=" + drDataType.getType(), coreLogger.ERROR);
				// Send on the results anyway
				//data.dataType = drDataType;
				ingestJob.data=dataResource;
			}
			// For exceptions with json parsing errors then just send the
			// text that was provided back
		//} catch (JsonProcessingException  jpe) {	
		} catch (Exception  jpe) {	

			dataResource = new DataResource();
			LOGGER.debug(jpe.toString());
			coreLogger.log(jpe.toString(), coreLogger.ERROR);
			TextDataType tr = new TextDataType();
			tr.content = serviceControlString;
			dataResource.dataType = tr;
			ingestJob.data=dataResource;
			
		} 
			
		// Host the data for now, will work on this later
		ingestJob.host = true;
		
		pjr.jobType  = ingestJob;
		
		// TODO Generate 123-456 with UUIDGen
		ProducerRecord<String,String> newProdRecord =
		//JobMessageFactory.getRequestJobMessage(pjr, uuidFactory.getUUID());	
	    JobMessageFactory.getRequestJobMessage(pjr, uuidGenerator.getUUID());	

		producer.send(newProdRecord);
		
		StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
		
	    // Create a text result and update status
		DataResult textResult = new DataResult(dataResource.dataId);

		statusUpdate.setResult(textResult);
		

		ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate);

		producer.send(prodRecord);
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
