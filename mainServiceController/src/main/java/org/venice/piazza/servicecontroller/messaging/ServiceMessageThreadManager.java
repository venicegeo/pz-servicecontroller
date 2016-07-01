package org.venice.piazza.servicecontroller.messaging;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import messaging.job.WorkerCallback;
import model.job.Job;
import model.job.PiazzaJobType;
import model.status.StatusUpdate;
import util.PiazzaLogger;

@Component
public class ServiceMessageThreadManager {

	private String DELETE_SERVICE_JOB_TOPIC_NAME;
	private String EXECUTE_SERVICE_JOB_TOPIC_NAME;
	private String READ_SERVICE_JOB_TOPIC_NAME;
	private String REGISTER_SERVICE_JOB_TOPIC_NAME;
	private String UPDATE_SERVICE_JOB_TOPIC_NAME;
	private String LIST_SERVICE_JOB_TOPIC_NAME;
	private String SEARCH_SERVICE_JOB_TOPIC_NAME;


	private String KAFKA_HOST;
	private String KAFKA_PORT;
	private String KAFKA_GROUP;
		
	/*
	 * TODO need to determine how statuses will be sent to update the job (Call back?)
	 */
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private List<String> topics;
	private final AtomicBoolean closed;

	private Map<String, Future<?>> runningServiceRequests;
	
	@Value("${SPACE}")
	private String SPACE;

	@Autowired
	private CoreServiceProperties coreServiceProperties;

	@Autowired
	private PiazzaLogger coreLogger;

	@Autowired
	ServiceMessageWorker serviceMessageWorker;

	/**
	 * Constructor for ServiceMessageThreadManager
	 */
	public ServiceMessageThreadManager() {
		closed = makeAtomicBoolean();

	}

	/**
	 * Initializing stuff 
	 */
	@PostConstruct
	public void initialize() {
		
		// Initialize dynamic topic names
		DELETE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "delete-service", SPACE);
		EXECUTE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "execute-service", SPACE);
		READ_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "read-service", SPACE);
		REGISTER_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "register-service", SPACE);
		UPDATE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "update-service", SPACE);
		LIST_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "list-service", SPACE);
		SEARCH_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "search-service", SPACE);

		topics = Arrays.asList(DELETE_SERVICE_JOB_TOPIC_NAME, EXECUTE_SERVICE_JOB_TOPIC_NAME, READ_SERVICE_JOB_TOPIC_NAME,
				REGISTER_SERVICE_JOB_TOPIC_NAME, UPDATE_SERVICE_JOB_TOPIC_NAME, LIST_SERVICE_JOB_TOPIC_NAME, SEARCH_SERVICE_JOB_TOPIC_NAME);

		// Initialize the Kafka consumer/producer
		String kafkaHostFull = coreServiceProperties.getKafkaHost();
		KAFKA_GROUP = coreServiceProperties.getKafkaGroup();

		KAFKA_HOST = kafkaHostFull.split(":")[0];
		KAFKA_PORT = kafkaHostFull.split(":")[1];

		coreLogger.log("============================================================", PiazzaLogger.INFO);
		coreLogger.log("DELETE_SERVICE_JOB_TOPIC_NAME=" + DELETE_SERVICE_JOB_TOPIC_NAME, PiazzaLogger.INFO);
		coreLogger.log("EXECUTE_SERVICE_JOB_TOPIC_NAME=" + EXECUTE_SERVICE_JOB_TOPIC_NAME, PiazzaLogger.INFO);
		coreLogger.log("READ_SERVICE_JOB_TOPIC_NAME=" + READ_SERVICE_JOB_TOPIC_NAME, PiazzaLogger.INFO);
		coreLogger.log("REGISTER_SERVICE_JOB_TOPIC_NAME=" + REGISTER_SERVICE_JOB_TOPIC_NAME, PiazzaLogger.INFO);
		coreLogger.log("UPDATE_SERVICE_JOB_TOPIC_NAME=" + UPDATE_SERVICE_JOB_TOPIC_NAME, PiazzaLogger.INFO);
		coreLogger.log("LIST_SERVICE_JOB_TOPIC_NAME=" + LIST_SERVICE_JOB_TOPIC_NAME, PiazzaLogger.INFO);
		coreLogger.log("SEARCH_SERVICE_JOB_TOPIC_NAME=" + SEARCH_SERVICE_JOB_TOPIC_NAME, PiazzaLogger.INFO);
		coreLogger.log("KAFKA_GROUP=" + KAFKA_GROUP, PiazzaLogger.INFO);
		coreLogger.log("KAFKA_HOST=" + KAFKA_HOST, PiazzaLogger.INFO);
		coreLogger.log("KAFKA_PORT=" + KAFKA_PORT, PiazzaLogger.INFO);
		coreLogger.log("============================================================", PiazzaLogger.INFO);

		/* Initialize producer and consumer for the Kafka Queue */
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);

		// Initialize the HashMap
		runningServiceRequests = new HashMap<String, Future<?>>();

		// Start polling for Kafka Jobs on the Group Consumer.. This occurs on a
		// separate Thread so as not to block Spring.
		Thread kafkaListenerThread = new Thread() {
			@Override
			public void run() {
				pollServiceJobs();
			}
		};

		// Subscribe for the topics
		consumer.subscribe(topics);
		kafkaListenerThread.start();

		// Start polling for Kafka Abort Jobs on the unique Consumer.	
		Thread pollAbortThread = new Thread() {
			@Override
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

		ObjectMapper mapper = makeObjectMapper();
		try {
			Job job;

			// Create a Callback that will be invoked when a Worker completes.
			// This will
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
					try {
						job = mapper.readValue(consumerRecord.value(), Job.class);

						if (job != null) {
							// Update the status to say the job is in progress
							StatusUpdate su = new StatusUpdate();
							su.setStatus(StatusUpdate.STATUS_RUNNING);

							ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(
									String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), job.getJobId(),
									mapper.writeValueAsString(su));
							producer.send(prodRecord);

							// Now get the job type and process the request
							PiazzaJobType jobType = job.getJobType();
							if (jobType != null) {
								coreLogger.log("Received jobType: " + jobType.getClass().getSimpleName(), PiazzaLogger.INFO);
							}

							// start a new thread
							Future<?> workerFuture = serviceMessageWorker.run(consumerRecord, producer, job, callback);

							// Keep track of all Running Jobs
							runningServiceRequests.put(consumerRecord.key(), workerFuture);
						}

					} catch (Exception ex) {
						coreLogger.log(String.format("The item received did not marshal to a job", ex), PiazzaLogger.FATAL);
					}
				} // for loop
			} // while loop
		} catch (Exception ex) {
			coreLogger.log(String.format("The item received did not marshal to a job", ex), PiazzaLogger.FATAL);

		}

	}

	/**
	 * Begins listening for Abort Jobs. If a Job is owned by this component,
	 * then it will be terminated.
	 */
	public void pollAbortServiceJobs() {
		Consumer<String, String> uniqueConsumer;
		uniqueConsumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT,
				String.format("%s-%s", KAFKA_GROUP, UUID.randomUUID().toString()));
		try {
			// Create the Unique Consumer

			uniqueConsumer.subscribe(Arrays.asList(String.format("%s-%s", JobMessageFactory.ABORT_JOB_TOPIC_NAME, SPACE)));

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
		} catch (WakeupException wex) {
			coreLogger.log(String.format("Polling Thread forcefully closed: %s", wex.getMessage()), PiazzaLogger.FATAL);
			uniqueConsumer.close();
		} catch (Exception ex) {
			coreLogger.log(String.format("Polling Thread forcefully closed: %s", ex.getMessage()), PiazzaLogger.FATAL);
			uniqueConsumer.close();
		}
	}
	
	public ObjectMapper makeObjectMapper() {
		return new ObjectMapper();
	}
	
	public AtomicBoolean makeAtomicBoolean () {
		return new AtomicBoolean();
	}
}
