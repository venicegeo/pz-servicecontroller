package org.venice.piazza.servicecontroller.messaging.handlers;
import java.util.List;

// TODO Add License
import model.job.PiazzaJobType;

import org.springframework.http.ResponseEntity;

/**
 * Interface for pz-servicecontroller message handlers
 * @author mlynum
 * @version 1.0
 *
 */
public interface PiazzaJobHandler {
	
	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest );
	

}
