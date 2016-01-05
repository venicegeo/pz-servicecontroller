package org.venice.piazza.serviceregistry.data.mongodb.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.venice.piazza.serviceregistry.model.Service;

public interface ServiceRepository extends MongoRepository<Service, Long> {
	
	public Service findByName(String name);
	public Service findById(Long id);
	

}