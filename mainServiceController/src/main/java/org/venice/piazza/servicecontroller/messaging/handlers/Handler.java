package org.venice.piazza.servicecontroller.messaging.handlers;
// TODO Add License
import model.job.PiazzaJobType;
/**
 * Interface for pz-servicecontroller message handlers
 * @author mlynum
 *
 */
public interface Handler {
	
	public void handle (PiazzaJobType jobRequest );

}
