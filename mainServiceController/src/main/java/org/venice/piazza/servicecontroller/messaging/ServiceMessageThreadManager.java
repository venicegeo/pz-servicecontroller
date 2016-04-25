package org.venice.piazza.servicecontroller.messaging;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;




import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import messaging.job.WorkerCallback;
import model.job.Job;
import model.job.PiazzaJobType;
import model.job.result.type.ErrorResult;
import model.status.StatusUpdate;
import util.PiazzaLogger;
import util.UUIDFactory;

@Component
public class ServiceMessageThreadManager {


		private String DELETE_SERVICE_JOB_TOPIC_NAME;
		private String EXECUTE_SERVICE_JOB_TOPIC_NAME;
		private String READ_SERVICE_JOB_TOPIC_NAME;
		private String REGISTER_SERVICE_JOB_TOPIC_NAME;
		private String UPDATE_SERVICE_JOB_TOPIC_NAME;
		private String LIST_SERVICE_JOB_TOPIC_NAME;
		private String SEARCH_SERVICE_JOB_TOPIC_NAME;

		private final static Logger LOGGER = LoggerFactory.getLogger(ServiceMessageThreadManager.class);
		
		private String KAFKA_HOST;

		private String KAFKA_PORT;
		private String KAFKA_GROUP;

		
		/*
		  TODO need to determine how statuses will be sent to update the job  (Call back?)
		 */
		private Producer<String, String> producer;
		private Consumer<String, String> consumer;
		private List<String> topics;
		private final AtomicBoolean closed = new AtomicBoolean(false);
		
		
		private ThreadPoolExecutor executor;
		private Map<String, Future<?>> runningServiceRequests;
		private String space;

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
			// Initialize dynamic topic names
			space = coreServiceProperties.getSpace();
			DELETE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "delete-service", space);
			EXECUTE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "execute-service", space);
			READ_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "read-service", space);
			REGISTER_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "register-service", space);
			UPDATE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "update-service", space);
			LIST_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "list-service", space);
			SEARCH_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "search-service", space);
			
			topics = Arrays.asList(DELETE_SERVICE_JOB_TOPIC_NAME, EXECUTE_SERVICE_JOB_TOPIC_NAME, 
					   READ_SERVICE_JOB_TOPIC_NAME, REGISTER_SERVICE_JOB_TOPIC_NAME,
					   UPDATE_SERVICE_JOB_TOPIC_NAME,LIST_SERVICE_JOB_TOPIC_NAME, SEARCH_SERVICE_JOB_TOPIC_NAME);;
			
			// Initialize the Kafka consumer/producer

			String kafkaHostFull = coreServiceProperties.getKafkaHost();
			KAFKA_GROUP = coreServiceProperties.getKafkaGroup();
			
			KAFKA_HOST = kafkaHostFull.split(":")[0];
			KAFKA_PORT = kafkaHostFull.split(":")[1];
			
			LOGGER.info("============================================================");

			LOGGER.info("DELETE_SERVICE_JOB_TOPIC_NAME=" + DELETE_SERVICE_JOB_TOPIC_NAME);
			LOGGER.info("EXECUTE_SERVICE_JOB_TOPIC_NAME=" + EXECUTE_SERVICE_JOB_TOPIC_NAME);
			LOGGER.info("READ_SERVICE_JOB_TOPIC_NAME=" + READ_SERVICE_JOB_TOPIC_NAME);
			LOGGER.info("REGISTER_SERVICE_JOB_TOPIC_NAME=" + REGISTER_SERVICE_JOB_TOPIC_NAME);
			LOGGER.info("UPDATE_SERVICE_JOB_TOPIC_NAME=" + UPDATE_SERVICE_JOB_TOPIC_NAME);
			LOGGER.info("LIST_SERVICE_JOB_TOPIC_NAME=" + LIST_SERVICE_JOB_TOPIC_NAME);
			LOGGER.info("SEARCH_SERVICE_JOB_TOPIC_NAME=" + SEARCH_SERVICE_JOB_TOPIC_NAME);
			LOGGER.info("KAFKA_GROUP=" + KAFKA_GROUP);
			LOGGER.info("KAFKA_HOST=" + KAFKA_HOST);
			LOGGER.info("KAFKA_PORT=" + KAFKA_PORT);			
			LOGGER.info("============================================================");


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
						//LOGGER.info("Received topic: " + consumerRecord.topic() + " with key "
						//		+ consumerRecord.key());
						
						//coreLogger.log("Received topic: " + consumerRecord.topic() + " with key "
						//		+ consumerRecord.key(), coreLogger.INFO);
												
						// Wrap the JobRequest in the Job object
						try {
							job = mapper.readValue(consumerRecord.value(), Job.class);	
							
							if (job != null) {
								// Update the status to say the job is in progress
								StatusUpdate su = new StatusUpdate();
								su.setStatus(StatusUpdate.STATUS_RUNNING);

								
								ProducerRecord<String,String> prodRecord =
                                        new ProducerRecord<String,String> (String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, space),job.getJobId(),
                                                mapper.writeValueAsString(su));
								producer.send(prodRecord);
								
								// Now get the job type and process the request
								PiazzaJobType jobType = job.getJobType();
								if (jobType != null) {
								  LOGGER.info("Received jobType: " + jobType.getType());
								  coreLogger.log("Received jobType: " + jobType.getType(), coreLogger.INFO);
								}


								ServiceMessageWorker serviceMessageWorker = new ServiceMessageWorker(consumerRecord, producer, accessor,  
															callback,coreServiceProperties, uuidFactory, coreLogger, job, space);
	
	
								Future<?> workerFuture = executor.submit(serviceMessageWorker);
	
								// Keep track of all Running Jobs
								runningServiceRequests.put(consumerRecord.key(), workerFuture);
								
							}

						} catch (Exception ex) {
							
						}
					}// for loop
				}// while loop
			} catch (Exception ex) {
				LOGGER.error("Problem", ex);
				coreLogger.log(String.format("There was a problem.", ex),
						PiazzaLogger.FATAL);
				
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
				uniqueConsumer.subscribe(Arrays.asList(String.format("%s-%s", JobMessageFactory.ABORT_JOB_TOPIC_NAME, space)));

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
	
		
		
			
		
}
