package org.venice.piazza.servicecontroller.messaging;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
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
import model.job.type.ExecuteServiceJob;
import model.job.type.IngestJob;
import model.request.PiazzaJobRequest;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.ParamDataItem;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import util.PiazzaLogger;
import util.UUIDFactory;

@Component
public class ServiceMessageThreadManager {
	// Jobs to listen to
		private static final String DELETE_SERVICE_JOB_TOPIC_NAME = "delete-service";
		private static final String EXECUTE_SERVICE_JOB_TOPIC_NAME = "execute-service";
		private static final String READ_SERVICE_JOB_TOPIC_NAME = "read-service";
		private static final String REGISTER_SERVICE_JOB_TOPIC_NAME = "register-service";
		private static final String UPDATE_SERVICE_JOB_TOPIC_NAME = "update-service";
		private static final String LIST_SERVICE_JOB_TOPIC_NAME = "list-service";
		private static final String SEARCH_SERVICE_JOB_TOPIC_NAME = "search-service";

		private final static Logger LOGGER = LoggerFactory.getLogger(ServiceMessageThreadManager.class);
		
		private String KAFKA_HOST;

		private String KAFKA_PORT;
		private String KAFKA_GROUP;

		
		/*
		  TODO need to determine how statuses will be sent to update the job  (Call back?)
		 */
		private Producer<String, String> producer;
		private Consumer<String, String> consumer;
		private List<String> topics = Arrays.asList(DELETE_SERVICE_JOB_TOPIC_NAME, EXECUTE_SERVICE_JOB_TOPIC_NAME, 
				   READ_SERVICE_JOB_TOPIC_NAME, REGISTER_SERVICE_JOB_TOPIC_NAME,
				   UPDATE_SERVICE_JOB_TOPIC_NAME,LIST_SERVICE_JOB_TOPIC_NAME, SEARCH_SERVICE_JOB_TOPIC_NAME);;
		private final AtomicBoolean closed = new AtomicBoolean(false);
		private RegisterServiceHandler rsHandler;
		private ExecuteServiceHandler esHandler;
		private UpdateServiceHandler usHandler;
		private DescribeServiceHandler dsHandler;
		private DeleteServiceHandler dlHandler;
		private ListServiceHandler lsHandler;
		private SearchServiceHandler ssHandler;
		
		
		private ThreadPoolExecutor executor;
		private Map<String, Future<?>> runningServiceRequests;

		@Autowired
		private MongoAccessor accessor;
		@Autowired
		private CoreServiceProperties coreServiceProperties;
		
		@Autowired
		private PiazzaLogger coreLogger;
		
		@Autowired
		private UUIDFactory uuidFactory;


		/**
		 * Constructor for ServiceMessageThreadManager
		 */
		public ServiceMessageThreadManager() { 

		}

		/**+
		 * 
		 */
		@PostConstruct
		public void initialize() {
			// Initialize the Kafka consumer/producer

			String kafkaHostFull = coreServiceProperties.getKafkaHost();
			KAFKA_GROUP = coreServiceProperties.getKafkaGroup();
			
			KAFKA_HOST = kafkaHostFull.split(":")[0];
			KAFKA_PORT = kafkaHostFull.split(":")[1];
			
			LOGGER.info("=================================");

			LOGGER.info("The KAFKA Host Properties is " + coreServiceProperties.getKafkaHost());
			LOGGER.info("The KAFKA Group Properties is " + coreServiceProperties.getKafkaGroup());
			LOGGER.info("=================================");


			/* Initialize producer and consumer for the Kafka Queue */

			producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
			consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);

			// Initialize the HashMap
			runningServiceRequests = new HashMap<String, Future<?>>();

			// Initialize the ThreadPoolManager
			executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
			
			// Start polling for Kafka Jobs on the Group Consumer.. This occurs on a
			// separate Thread so as not to block Spring.
			Thread kafkaListenerThread = new Thread() {
				public void run() {
					pollServiceJobs();
				}
			};
			// Subscribe for the topics
			consumer.subscribe(topics);
			kafkaListenerThread.start();
			
			// Start polling for Kafka Abort Jobs on the unique Consumer.
			Thread pollAbortThread = new Thread() {
				public void run() {
					pollAbortServiceJobs();
				}
			};
			pollAbortThread.start();
		}
	   /**
	    * Polls for service controller topics and handles these requests.
	    */
		public void pollServiceJobs() {
			
			ObjectMapper mapper = new ObjectMapper();
			try {
				Job job = null;
			
				// Create a Callback that will be invoked when a Worker completes. This will
				WorkerCallback callback = new WorkerCallback() {
					@Override
					public void onComplete(String jobId) {
						runningServiceRequests.remove(jobId);
					}
				};
				while (!closed.get()) {
					ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
					// Handle new Messages on this topic.
					for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
						LOGGER.info("Received topic: " + consumerRecord.topic() + " with key "
								+ consumerRecord.key());
						//coreLogger.log("Received topic: " + consumerRecord.topic() + " with key "
						//		+ consumerRecord.key(), coreLogger.INFO);
												
						// Wrap the JobRequest in the Job object
						try {
							job = mapper.readValue(consumerRecord.value(), Job.class);	
							
							if (job != null) {
								PiazzaJobType jobType = job.getJobType();

								ExecuteServiceJob jobItem = (ExecuteServiceJob)jobType;
								ExecuteServiceData esData = jobItem.data;
								DataType dataType= esData.getDataOutput();

								ServiceMessageWorker serviceMessageWorker = new ServiceMessageWorker(consumerRecord, producer, accessor,  
															callback,coreServiceProperties, uuidFactory, coreLogger, job);
	
	
								Future<?> workerFuture = executor.submit(serviceMessageWorker);
	
								// Keep track of all Running Jobs
								runningServiceRequests.put(consumerRecord.key(), workerFuture);
								
							}

						} catch (Exception ex) {
							
						}
					}// for loop
				}// while loop
			} catch (Exception ex) {
				//coreLogger.log(String.format("There was a problem.", ex.getMessage()),
				//		PiazzaLogger.FATAL);
				
			}
			
		}
		
		/**
		 * Begins listening for Abort Jobs. If a Job is owned by this component,
		 * then it will be terminated.
		 */
		public void pollAbortServiceJobs() {
			try {
				// Create the Unique Consumer
				
				Consumer<String, String> uniqueConsumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT,

						String.format("%s-%s", KAFKA_GROUP, UUID.randomUUID().toString()));
				uniqueConsumer.subscribe(Arrays.asList(JobMessageFactory.ABORT_JOB_TOPIC_NAME));

				// Poll
				while (!closed.get()) {
					ConsumerRecords<String, String> consumerRecords = uniqueConsumer.poll(1000);
					// Handle new Messages on this topic.
					for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
						// Determine if this Job ID is being processed by this
						// component.
						String jobId = consumerRecord.key();
						if (runningServiceRequests.containsKey(jobId)) {
							// Cancel the Running Job
							runningServiceRequests.get(jobId).cancel(true);
							// Remove it from the list of Running Jobs
							runningServiceRequests.remove(jobId);
						}
					}
				}
			} catch (WakeupException ex) {
				coreLogger.log(String.format("Polling Thread forcefully closed: %s", ex.getMessage()),
						PiazzaLogger.FATAL);
			}
		}
		/**
		 *  This method is for demonstrating ingest of raster data
		 *  This will be refactored once the API changes have been communicated to
		 *  other team members
		 */
		public void handleRasterType(ExecuteServiceJob executeJob, Job job) {
			
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
				       
					    // END CALLING SERVICE CALL

					   // String tempString = "{\"dataType\":{\"type\":\"raster\",\"location\":{\"type\":\"s3\",\"bucketName\":\"pz-svcs-prevgen\",\"fileName\":\"c4226046-e20d-450e-a2ae-04eec7bce5e0-NASA-GDEM-10km-colorized.tif\",\"domainName\":\"s3.amazonaws.com\",\"type\":\"s3\"}},\"metadata\":{\"name\":\"External Crop Raster Service\"}}";
					   // DOES NOT WORK, BLOCKS FOR EVER
//					   String tempString = "{" +
//					        "\"dataType\": {" +
//					           "\"type\": \"raster\","+
//					            "\"location\": {"+
//					                "\"type\": \"s3\","+
//					                "\"bucketName\": \"pz-svcs-prevgen\","+
//					                "\"fileName\": \"27d26a9b-3f42-453e-914d-05d4cb6a4445-NASA-GDEM-10km-colorized.tif\"," +
//					               "\"domainName\": \"s3.amazonaws.com\""+
//					           " },"+
//					           "\"type\": \"raster\""+
//					        "},"+
//					        "\"metadata\": {" +
//					            "\"name\": \"External Crop Raster Service\""+
//					        "}" +
//					    "}";  
				        // Does not Work BLOCKS FOREVER
//				        String tempString = "{" +
//						        "\"dataType\": {" +
//						           "\"type\": \"raster\","+
//						           "\"location\": {"+
//					                "\"type\": \"s3\","+
//					                "\"bucketName\": \"pz-svcs-prevgen\""+
//					             " }" +
//						        "},"+
//						        "\"metadata\": {" +
//						            "\"name\": \"External Crop Raster Service\""+
//						        "}" +
//						    "}";  
				        // Does not Work - BLOCKS FOREVER
				        String tempString = "{" +
						        "\"dataType\": {" +
						           "\"type\": \"raster\","+
						        "\"location\": {"+
					                "\"type\": \"s3\""+
					             " }" +
						        "},"+
						        "\"metadata\": {" +
						       "\"name\": \"External Crop Raster Service\""+
						        "}" +
						    "}";
				        // Works
//				        String tempString = "{" +
//						        "\"dataType\": {" +
//						           "\"type\": \"raster\""+
//						        "},"+
//						        "\"metadata\": {" +
//						            "\"name\": \"External Crop Raster Service\""+
//						        "}" +
//						    "}";
				        LOGGER.debug("Temp String " + tempString);
				        // Works Too
				        //String tempString = "{\"dataType\":{\"type\":\"text\",\"type\":\"text\"}}";
				     
				        ObjectMapper tempMapper = new ObjectMapper();
					    DataResource dataResource = tempMapper.readValue(serviceControlString, DataResource.class);
				        
				        LOGGER.debug("This is a test");
				        LOGGER.debug("dataResource type is" + dataResource.getDataType().getType());

					    //DataResource dataResource = new DataResource();
			            dataResource.dataId = uuidFactory.getUUID();
			            LOGGER.debug("dataId" + dataResource.dataId);
			            PiazzaJobRequest pjr  =  new PiazzaJobRequest();
			            pjr.apiKey = "pz-sc-ingest-raster-test";
			            
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
		
			
		
}
