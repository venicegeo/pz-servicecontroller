package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license

import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.data.model.UUID;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.PiazzaJobHandler;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.type.RegisterServiceJob;


/**
 * Handler for handling registerService requests.  This handler is used 
 * when register-service kafka topics are received or when clients utilize the 
 * ServiceController registerService web service.
 * @author mlynum
 * @version 1.0
 *
 */

public class RegisterServiceHandler implements PiazzaJobHandler {
	@Autowired
	private MongoAccessor accessor;
	

	private CoreLogger coreLogger;
	private RestTemplate template;
	private CoreServiceProperties coreServiceProperties;
	private static final Logger LOGGER = LoggerFactory.getLogger(RegisterServiceHandler.class);


	public RegisterServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProp, CoreLogger coreLogger){ 
		this.accessor = accessor;
		this.coreServiceProperties = coreServiceProp;
		this.template = new RestTemplate();
		this.coreLogger = coreLogger;
	}

    /*
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public void handle (PiazzaJobType jobRequest ) {
		
		LOGGER.debug("Registering a service");
		RegisterServiceJob job = (RegisterServiceJob)jobRequest;
		if (job != null)  {
			// Get the ResourceMetadata
			model.job.metadata.ResourceMetadata rMetadata = job.data;

			String result = handle(rMetadata);
			if (result.length() > 0) {
				String jobId = job.getJobId();
				// TODO Use the result, send a message with the resource ID
				// and jobId
				
			}
			else {
				LOGGER.error("No result response from the handler, something went wrong");
			}
		}
	}//handle
	
	/**
	 * 
	 * @param rMetadata
	 * @return resourceID of the registered service
	 */
	public String handle (ResourceMetadata rMetadata) {
		
		try {
			ResponseEntity<UUID> uuid = template.postForEntity("http://" + coreServiceProperties.getUuidservice(), null, UUID.class);
			List <String> data = uuid.getBody().getData();
			
			if (data != null )
			{
				LOGGER.debug("Response from UUIDgen" + uuid.toString());
				if (data.size() > 1) {
		
				LOGGER.debug("Received more than one ID from the UUIDGen service, " +
							"defaulting to first id returned.");
				}
				rMetadata.resourceId = data.get(0);
			} else {
				// No data came from the UUIDGen, generate own ID
				rMetadata.resourceId = generateId();
			}
		
			
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage());
			LOGGER.debug(ex.toString());
			// The UUID Gen Service is not accessible so now
			// Make up a random ID	
			rMetadata.resourceId = generateId();
			
		}
	
		String result = accessor.save(rMetadata);
		LOGGER.debug("The result of the save is " + result);
		if (result.length() > 0) {
		   coreLogger.log("The service " + rMetadata.name + " was stored with id " + result, CoreLogger.INFO);
		} else {
			   coreLogger.log("The service " + rMetadata.name + " was NOT stored", CoreLogger.INFO);
		}
		// If an ID was returned then send a kafka message back updating the job iD 
		// with the resourceID
		return result;
				
	}
	
	/**
	 * Generates an ID for persisting data using Random
	 * @return id 
	 */
	private String generateId() {
		String id = "";
		Random rand = new Random(System.nanoTime());
		int randomInt = rand.nextInt(1000000000);
		rand = new Random();		
		id= "123-345-456" + (new Integer(randomInt).toString()) + rand.nextInt(100) + 2;
		return id;
		
	}

}
