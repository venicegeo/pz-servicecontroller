package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO Add License
import model.job.PiazzaJobType;

/**
 * Interface for pz-servicecontroller message handlers
 * @author mlynum
 * @version 1.0
 *
 */
public interface PiazzaJobHandler {
	
	public void handle (PiazzaJobType jobRequest );
	

}
