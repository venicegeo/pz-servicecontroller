package org.venice.piazza.servicecontroller.data.mongodb.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.venice.piazza.servicecontroller.model.Service;

public interface ServiceRepository extends MongoRepository<Service, Long> {
	
	public Service findByName(String name);
	public Service findById(Long id);
	

}