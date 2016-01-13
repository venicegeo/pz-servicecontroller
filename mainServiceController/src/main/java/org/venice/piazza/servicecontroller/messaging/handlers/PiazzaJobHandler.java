package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO Add License
import model.job.PiazzaJobType;
//import model.job.metadata.ResourceMetadata;
import org.venice.piazza.servicecontroller.model.ResourceMetadata;
/**
 * Interface for pz-servicecontroller message handlers
 * @author mlynum
 *
 */
public interface PiazzaJobHandler {
	
	public void handle (PiazzaJobType jobRequest );
	

}
