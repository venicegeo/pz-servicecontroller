package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO add license


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.PiazzaJobHandler;
import org.venice.piazza.servicecontroller.util.CoreLogger;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import org.venice.piazza.servicecontroller.util.CoreUUIDGen;

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
	private MongoAccessor accessor;
	private CoreLogger coreLogger;
	private CoreUUIDGen coreUuidGen;
	private static final Logger LOGGER = LoggerFactory.getLogger(RegisterServiceHandler.class);


	public RegisterServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProp, CoreLogger coreLogger, CoreUUIDGen coreUuidGen){ 
		this.accessor = accessor;
		this.coreLogger = coreLogger;
		this.coreUuidGen = coreUuidGen;
	
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

        coreLogger.log("about to save a registered service.", CoreLogger.INFO);

		rMetadata.id = coreUuidGen.getUUID();
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
	


}
