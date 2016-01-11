package org.venice.piazza.servicecontroller.controller;

import org.springframework.web.bind.annotation.RestController;
import org.venice.piazza.servicecontroller.data.mongodb.repository.ServiceRepository;
import org.venice.piazza.servicecontroller.model.Service;

import messaging.job.KafkaClientFactory;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;

@Component
public class ServiceController {

	public ServiceController() {
		
	}
	
	@PostConstruct
	public void initialize() {
		// Initialize the Consumer and Producer
	
	}
    
	
	
}
