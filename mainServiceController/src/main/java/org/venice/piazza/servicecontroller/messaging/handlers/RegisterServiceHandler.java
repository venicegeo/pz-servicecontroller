package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license



import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.CoreServiceProperties;
import org.venice.piazza.servicecontroller.data.model.UUID;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.PiazzaJobHandler;

import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.type.RegisterServiceJob;

public class RegisterServiceHandler implements PiazzaJobHandler {
	@Autowired
	private MongoAccessor accessor;
	private RestTemplate template;
	private CoreServiceProperties coreServiceProp;
	private final static Logger LOGGER = Logger.getLogger(RegisterServiceHandler.class);


	public RegisterServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProp) {
		this.accessor = accessor;
		this.coreServiceProp = coreServiceProp;
		this.template = new RestTemplate();
	}

    /*
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public void handle (PiazzaJobType jobRequest ) {
		
		
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
	
	public String handle (ResourceMetadata rMetadata) {
		
		try {
			ResponseEntity<UUID> uuid = template.postForEntity("http://" + coreServiceProp.getUuidservice(), null, UUID.class);
			List <String> data = uuid.getBody().getData();
			
			if (data != null )
			{
				LOGGER.info("Response from UUIDgen" + uuid.toString());
				System.out.println(uuid.toString());
				if (data.size() > 1) {
		
				LOGGER.info("Received more than one ID from the UUIDGen service, " +
							"defaulting to first id returned.");
				}
				rMetadata.resourceId = data.get(0);
			} else {
				// No data came from the UUIDGen, generate own ID
				rMetadata.resourceId = generateId();
			}
		
			
		} catch (RestClientException ex) {
			ex.printStackTrace();
			LOGGER.error(ex.getMessage());
			// The UUID Gen Service is not accessible so now
			// Make up a random ID	
			rMetadata.resourceId = generateId();
			
		}
		
		// TODO Now call the UUID generator service
		// http://localhost:8080/uuid?count="2" returns a two uuid
		
	
		String result = accessor.save(rMetadata);
		LOGGER.info("THe result of the save is " + result);
		System.out.println("The result is " + result);
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
		Random rand = new Random();		
		id= "123-345-456" + rand.nextInt(100) + 2;
		return id;
		
	}

}
