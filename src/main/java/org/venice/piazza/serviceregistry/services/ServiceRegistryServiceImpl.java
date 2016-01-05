package org.venice.piazza.serviceregistry.services;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.venice.piazza.serviceregistry.data.mongodb.repository.ServiceRepository;
import org.springframework.stereotype.Service;

@Service
public class ServiceRegistryServiceImpl implements ServiceRegistryService {
	
	@Autowired
	private ServiceRepository repository;
	public void addService(org.venice.piazza.serviceregistry.model.Service service) {
		
		repository.save(service);
		

	}
	
	public List<org.venice.piazza.serviceregistry.model.Service> getAllServices() {
		return repository.findAll();
	}

}
