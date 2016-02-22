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
package org.venice.piazza.servicecontroller.messaging;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.data.DataResource;
import model.data.type.TextResource;
import model.job.Job;
import model.job.PiazzaJobType;
import model.job.result.type.DataResult;
import model.job.result.type.ErrorResult;
import model.job.result.type.TextResult;
import model.job.type.DeleteServiceJob;
import model.job.type.DescribeServiceMetadataJob;
import model.job.type.ExecuteServiceJob;

import model.job.type.ListServicesJob;
import model.job.type.IngestJob;
import model.job.type.RegisterServiceJob;
import model.request.PiazzaJobRequest;
import model.job.type.UpdateServiceJob;
import model.status.StatusUpdate;
import util.PiazzaLogger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.DeleteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.DescribeServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ListServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.UpdateServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import org.venice.piazza.servicecontroller.util.CoreUUIDGen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * Purpose of this controller is to register for the Kafka messages and listen for service controller topics.
 * @author mlynum
 * @version 1.0
 */


@Controller
@DependsOn("coreInitDestroy")
public class ServiceControllerMessageHandler implements Runnable {
	// Jobs to listen to
	private static final String DELETE_SERVICE_JOB_TOPIC_NAME = "delete-service";
	private static final String EXECUTE_SERVICE_JOB_TOPIC_NAME = "execute-service";
	private static final String READ_SERVICE_JOB_TOPIC_NAME = "read-service";
	private static final String REGISTER_SERVICE_JOB_TOPIC_NAME = "register-service";
	private static final String UPDATE_SERVICE_JOB_TOPIC_NAME = "update-service";
	private static final String List_SERVICE_JOB_TOPIC_NAME = "list-service";
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceControllerMessageHandler.class);
	
	private String KAFKA_HOST;
	private int KAFKA_PORT;
	private String KAFKA_GROUP;
	/*
	  TODO need to determine how statuses will be sent to update the job  (Call back?)
	 */
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private List<String> topics;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private RegisterServiceHandler rsHandler;
	private ExecuteServiceHandler esHandler;
	private UpdateServiceHandler usHandler;
	private DescribeServiceHandler dsHandler;
	private DeleteServiceHandler dlHandler;
	private ListServiceHandler lsHandler;

	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private CoreServiceProperties coreServiceProperties;
	
	@Autowired
	private PiazzaLogger coreLogger;
	
	@Autowired
	private CoreUUIDGen coreUuidGen;

	/**
	 * Constructor
	 */
	public ServiceControllerMessageHandler() {
		topics = Arrays.asList(DELETE_SERVICE_JOB_TOPIC_NAME, EXECUTE_SERVICE_JOB_TOPIC_NAME, 
							   READ_SERVICE_JOB_TOPIC_NAME, REGISTER_SERVICE_JOB_TOPIC_NAME,
							   UPDATE_SERVICE_JOB_TOPIC_NAME,List_SERVICE_JOB_TOPIC_NAME);

	}

	/**+
	 * 
	 */
	@PostConstruct
	public void initialize() {
		// Initialize the Kafka consumer/producer
		KAFKA_PORT = coreServiceProperties.getKafkaPort();
		KAFKA_HOST = coreServiceProperties.getKafkaHost();
		KAFKA_GROUP = coreServiceProperties.getKafkaGroup();
		LOGGER.info("=================================");
		LOGGER.info("The KAFKA Port Properties is " + coreServiceProperties.getKafkaPort());
		LOGGER.info("The KAFKA Host Properties is " + coreServiceProperties.getKafkaHost());
		LOGGER.info("The KAFKA Group Properties is " + coreServiceProperties.getKafkaGroup());
		 // Initialize the handlers to handle requests from the message queue
		rsHandler = new RegisterServiceHandler(accessor, coreServiceProperties, coreLogger, coreUuidGen);
		usHandler = new UpdateServiceHandler(accessor, coreServiceProperties, coreLogger, coreUuidGen);
		dlHandler = new DeleteServiceHandler(accessor, coreServiceProperties, coreLogger, coreUuidGen);
		esHandler = new ExecuteServiceHandler(accessor, coreServiceProperties, coreLogger);
		dsHandler = new DescribeServiceHandler(accessor, coreServiceProperties, coreLogger);
		lsHandler = new ListServiceHandler(accessor, coreServiceProperties, coreLogger);
		LOGGER.info("=================================");

		String KAFKA_PORT_STRING = new Integer(KAFKA_PORT).toString();
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT_STRING);
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT_STRING, KAFKA_GROUP);
		// Start the runner that will relay Job Creation topics.
		Thread kafkaListenerThread = new Thread(this);
		
		// Subscribe for the topics
		consumer.subscribe(topics);
		kafkaListenerThread.start();
	}

	@Override
	public void run() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			Job job = null;
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					LOGGER.info("Relaying Message " + consumerRecord.topic() + " with key "
							+ consumerRecord.key());
					// Wrap the JobRequest in the Job object
					String handleUpdate = StatusUpdate.STATUS_SUCCESS;
					String handleTextUpdate = "";
					ResponseEntity<List<String>> handleResult = null;
					try {
						job = mapper.readValue(consumerRecord.value(), Job.class);
												
						
						PiazzaJobType jobType = job.jobType;
						
						// See what type of job was sent
						
						
						if (jobType instanceof RegisterServiceJob) {
						   // Handle Register Job
						   LOGGER.debug("Job ID:" + job.getJobId());
						   handleResult = rsHandler.handle(jobType);
						   handleResult = checkResult(handleResult);
						   LOGGER.debug("Job ID:" + job.getJobId());
						   sendRegisterStatus(job, handleUpdate, handleResult);
							
						} else if (jobType instanceof ExecuteServiceJob) {
						
							handleResult = esHandler.handle(jobType);
							handleResult = checkResult(handleResult);
							sendExecuteStatus(job, handleUpdate, handleResult);
						} 
						else if (jobType instanceof UpdateServiceJob) {
							   // Handle Register Job
							handleResult = usHandler.handle(jobType);
							handleResult = checkResult(handleResult);
							sendUpdateStatus(job, handleUpdate, handleResult);
							
						}
						else if (jobType instanceof DeleteServiceJob) {
							   // Handle Register Job
						    handleResult = dlHandler.handle(jobType);		
						}
						else if (jobType instanceof DescribeServiceMetadataJob) {
							   // Handle Register Job
						    handleResult = dsHandler.handle(jobType);
						}
						else if (jobType instanceof ListServicesJob) {
							   // Handle Register Job						  
						   handleResult = lsHandler.handle(jobType);
							
						}
						
						
						
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
					
				}
			}
			
			
		} catch (WakeupException ex) {
			LOGGER.error(ex.getMessage());
		} catch (JsonProcessingException ex) {
			LOGGER.error(ex.getMessage());
		}
		
		
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
	 * Sends an update for registering a job
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
	 * Send an execute job status and the resource that was used
	 * @param job
	 * @param status
	 * @param handleResult
	 * @throws JsonProcessingException
	 */
	private void sendExecuteStatus(Job job, String status, ResponseEntity<List<String>> handleResult)  throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		String serviceControlString = mapper.writeValueAsString(handleResult.getBody());
		// Now produce a new record
		PiazzaJobRequest pjr  =  new PiazzaJobRequest();		
		// TODO read from properties file
		pjr.apiKey = "pz-sc-ingest-test";
		IngestJob ingestJob = new IngestJob();						
		DataResource data = new DataResource();
		//TODO  MML UUIDGen
		data.dataId = coreUuidGen.getUUID();
		TextResource tr = new TextResource();
		tr.content = serviceControlString;
		data.dataType = tr;
		ingestJob.data=data;
		ingestJob.host = true;
		
		pjr.jobType  = ingestJob;
		
		// TODO Generate 123-456 with UUIDGen
		ProducerRecord<String,String> newProdRecord =
		JobMessageFactory.getRequestJobMessage(pjr, coreUuidGen.getUUID());	
		
		producer.send(newProdRecord);
		
		StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
		
	    // Create a text result and update status
		DataResult textResult = new DataResult(data.dataId);

		statusUpdate.setResult(textResult);
		

		ProducerRecord<String,String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate);

		producer.send(prodRecord);
	}


	
}

